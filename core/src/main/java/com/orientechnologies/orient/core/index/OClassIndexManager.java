/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.*;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;

import java.util.*;

/**
 * Handles indexing when records change.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com), Artem Orobets
 */
public class OClassIndexManager extends ODocumentHookAbstract {

  public OClassIndexManager(ODatabaseDocument database) {
    super(database);
  }

  @Override
  public SCOPE[] getScopes() {
    return new SCOPE[] { SCOPE.CREATE, SCOPE.UPDATE, SCOPE.DELETE };
  }

  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.BOTH;
  }

  @Override
  public void onRecordAfterCreate(ODocument document) {
    document = checkForLoading(document);

    final OClass cls = ODocumentInternal.getImmutableSchemaClass(document);
    if (cls != null) {
      final Collection<OIndex<?>> indexes = cls.getIndexes();
      addIndexesEntries(document, indexes);
    }
  }

  @Override
  public void onRecordAfterUpdate(ODocument iDocument) {
    iDocument = checkForLoading(iDocument);

    final OClass cls = ODocumentInternal.getImmutableSchemaClass(iDocument);
    if (cls == null)
      return;

    final Collection<OIndex<?>> indexes = cls.getIndexes();

    if (!indexes.isEmpty()) {
      final Set<String> dirtyFields = new HashSet<>(Arrays.asList(iDocument.getDirtyFields()));

      if (!dirtyFields.isEmpty())
        for (final OIndex<?> index : indexes)
          processIndexUpdate(iDocument, dirtyFields, index);
    }
  }

  @Override
  public void onRecordAfterDelete(final ODocument iDocument) {
    deleteIndexEntries(iDocument);
  }

  protected void putInIndex(OIndex<?> index, Object key, OIdentifiable value) {
    index.put(key, value);
  }

  protected void removeFromIndex(OIndex<?> index, Object key, OIdentifiable value) {
    index.remove(key, value);
  }

  private void processCompositeIndexUpdate(final OIndex<?> index, final Set<String> dirtyFields, final ODocument iRecord) {
    final OCompositeIndexDefinition indexDefinition = (OCompositeIndexDefinition) index.getDefinition();

    final List<String> indexFields = indexDefinition.getFields();
    final String multiValueField = indexDefinition.getMultiValueField();

    for (final String indexField : indexFields) {
      if (dirtyFields.contains(indexField)) {
        final List<Object> origValues = new ArrayList<>(indexFields.size());

        for (final String field : indexFields) {
          if (!field.equals(multiValueField))
            if (dirtyFields.contains(field)) {
              origValues.add(iRecord.getOriginalValue(field));
            } else {
              origValues.add(iRecord.field(field));
            }
        }

        if (multiValueField == null) {
          final Object origValue = indexDefinition.createValue(origValues);
          final Object newValue = indexDefinition.getDocumentValueToIndex(iRecord);

          if (!indexDefinition.isNullValuesIgnored() || origValue != null)
            removeFromIndex(index, origValue, iRecord);

          if (!indexDefinition.isNullValuesIgnored() || newValue != null)
            putInIndex(index, newValue, iRecord.getIdentity());
        } else {
          final OMultiValueChangeTimeLine<?, ?> multiValueChangeTimeLine = iRecord.getCollectionTimeLine(multiValueField);
          if (multiValueChangeTimeLine == null) {
            if (dirtyFields.contains(multiValueField))
              origValues.add(indexDefinition.getMultiValueDefinitionIndex(), iRecord.getOriginalValue(multiValueField));
            else
              origValues.add(indexDefinition.getMultiValueDefinitionIndex(), iRecord.field(multiValueField));

            final Object origValue = indexDefinition.createValue(origValues);
            final Object newValue = indexDefinition.getDocumentValueToIndex(iRecord);

            processIndexUpdateFieldAssignment(index, iRecord, origValue, newValue);
          } else {
            //in case of null values support and empty collection field we put null placeholder in
            //place where collection item should be located so we can not use "fast path" to
            //update index values
            if (dirtyFields.size() == 1 && indexDefinition.isNullValuesIgnored()) {
              final Map<OCompositeKey, Integer> keysToAdd = new HashMap<>();
              final Map<OCompositeKey, Integer> keysToRemove = new HashMap<>();

              for (OMultiValueChangeEvent<?, ?> changeEvent : multiValueChangeTimeLine.getMultiValueChangeEvents()) {
                indexDefinition.processChangeEvent(changeEvent, keysToAdd, keysToRemove, origValues.toArray());
              }

              for (final Object keyToRemove : keysToRemove.keySet())
                removeFromIndex(index, keyToRemove, iRecord);

              for (final Object keyToAdd : keysToAdd.keySet())
                putInIndex(index, keyToAdd, iRecord.getIdentity());
            } else {
              final OTrackedMultiValue fieldValue = iRecord.field(multiValueField);
              @SuppressWarnings("unchecked")
              final Object restoredMultiValue = fieldValue
                  .returnOriginalState(multiValueChangeTimeLine.getMultiValueChangeEvents());

              origValues.add(indexDefinition.getMultiValueDefinitionIndex(), restoredMultiValue);

              final Object origValue = indexDefinition.createValue(origValues);
              final Object newValue = indexDefinition.getDocumentValueToIndex(iRecord);

              processIndexUpdateFieldAssignment(index, iRecord, origValue, newValue);
            }
          }
        }
        return;
      }
    }
  }

  private void processSingleIndexUpdate(final OIndex<?> index, final Set<String> dirtyFields, final ODocument iRecord) {
    final OIndexDefinition indexDefinition = index.getDefinition();
    final List<String> indexFields = indexDefinition.getFields();

    if (indexFields.isEmpty())
      return;

    final String indexField = indexFields.get(0);
    if (!dirtyFields.contains(indexField))
      return;

    final OMultiValueChangeTimeLine<?, ?> multiValueChangeTimeLine = iRecord.getCollectionTimeLine(indexField);
    if (multiValueChangeTimeLine != null) {
      final OIndexDefinitionMultiValue indexDefinitionMultiValue = (OIndexDefinitionMultiValue) indexDefinition;
      final Map<Object, Integer> keysToAdd = new HashMap<>();
      final Map<Object, Integer> keysToRemove = new HashMap<>();

      for (OMultiValueChangeEvent<?, ?> changeEvent : multiValueChangeTimeLine.getMultiValueChangeEvents()) {
        indexDefinitionMultiValue.processChangeEvent(changeEvent, keysToAdd, keysToRemove);
      }

      for (final Object keyToRemove : keysToRemove.keySet())
        removeFromIndex(index, keyToRemove, iRecord);

      for (final Object keyToAdd : keysToAdd.keySet())
        putInIndex(index, keyToAdd, iRecord.getIdentity());

    } else {
      final Object origValue = indexDefinition.createValue(iRecord.getOriginalValue(indexField));
      final Object newValue = indexDefinition.getDocumentValueToIndex(iRecord);

      processIndexUpdateFieldAssignment(index, iRecord, origValue, newValue);
    }
  }

  private void processIndexUpdateFieldAssignment(OIndex<?> index, ODocument iRecord, final Object origValue,
      final Object newValue) {

    final OIndexDefinition indexDefinition = index.getDefinition();
    if ((origValue instanceof Collection) && (newValue instanceof Collection)) {
      final Set<Object> valuesToRemove = new HashSet<>((Collection<?>) origValue);
      final Set<Object> valuesToAdd = new HashSet<>((Collection<?>) newValue);

      valuesToRemove.removeAll((Collection<?>) newValue);
      valuesToAdd.removeAll((Collection<?>) origValue);

      for (final Object valueToRemove : valuesToRemove) {
        if (!indexDefinition.isNullValuesIgnored() || valueToRemove != null) {
          removeFromIndex(index, valueToRemove, iRecord);
        }
      }

      for (final Object valueToAdd : valuesToAdd) {
        if (!indexDefinition.isNullValuesIgnored() || valueToAdd != null) {
          putInIndex(index, valueToAdd, iRecord);
        }
      }
    } else {
      deleteIndexKey(index, iRecord, origValue);

      if (newValue instanceof Collection) {
        for (final Object newValueItem : (Collection<?>) newValue) {
          putInIndex(index, newValueItem, iRecord.getIdentity());
        }
      } else if (!indexDefinition.isNullValuesIgnored() || newValue != null) {
        putInIndex(index, newValue, iRecord.getIdentity());
      }
    }
  }

  private boolean processCompositeIndexDelete(final OIndex<?> index, final Set<String> dirtyFields, final ODocument iRecord) {
    final OCompositeIndexDefinition indexDefinition = (OCompositeIndexDefinition) index.getDefinition();

    final String multiValueField = indexDefinition.getMultiValueField();

    final List<String> indexFields = indexDefinition.getFields();
    for (final String indexField : indexFields) {
      // REMOVE IT
      if (dirtyFields.contains(indexField)) {
        final List<Object> origValues = new ArrayList<>(indexFields.size());

        for (final String field : indexFields) {
          if (!field.equals(multiValueField))
            if (dirtyFields.contains(field))
              origValues.add(iRecord.getOriginalValue(field));
            else
              origValues.add(iRecord.field(field));
        }

        if (multiValueField != null) {
          final OMultiValueChangeTimeLine<?, ?> multiValueChangeTimeLine = iRecord.getCollectionTimeLine(multiValueField);
          if (multiValueChangeTimeLine != null) {
            final OTrackedMultiValue fieldValue = iRecord.field(multiValueField);
            @SuppressWarnings("unchecked")
            final Object restoredMultiValue = fieldValue.returnOriginalState(multiValueChangeTimeLine.getMultiValueChangeEvents());
            origValues.add(indexDefinition.getMultiValueDefinitionIndex(), restoredMultiValue);
          } else if (dirtyFields.contains(multiValueField))
            origValues.add(indexDefinition.getMultiValueDefinitionIndex(), iRecord.getOriginalValue(multiValueField));
          else
            origValues.add(indexDefinition.getMultiValueDefinitionIndex(), iRecord.field(multiValueField));
        }

        final Object origValue = indexDefinition.createValue(origValues);
        deleteIndexKey(index, iRecord, origValue);

        return true;
      }
    }
    return false;
  }

  private void deleteIndexKey(final OIndex<?> index, final ODocument iRecord, final Object origValue) {
    final OIndexDefinition indexDefinition = index.getDefinition();

    if (origValue instanceof Collection) {
      for (final Object valueItem : (Collection<?>) origValue) {
        if (!indexDefinition.isNullValuesIgnored() || valueItem != null)
          removeFromIndex(index, valueItem, iRecord);
      }
    } else if (!indexDefinition.isNullValuesIgnored() || origValue != null) {
      removeFromIndex(index, origValue, iRecord);
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private boolean processSingleIndexDelete(final OIndex<?> index, final Set<String> dirtyFields, final ODocument iRecord) {
    final OIndexDefinition indexDefinition = index.getDefinition();

    final List<String> indexFields = indexDefinition.getFields();
    if (indexFields.isEmpty()) {
      return false;
    }

    final String indexField = indexFields.iterator().next();
    if (dirtyFields.contains(indexField)) {
      final OMultiValueChangeTimeLine<?, ?> multiValueChangeTimeLine = iRecord.getCollectionTimeLine(indexField);

      final Object origValue;
      if (multiValueChangeTimeLine != null) {
        final OTrackedMultiValue fieldValue = iRecord.field(indexField);
        final Object restoredMultiValue = fieldValue.returnOriginalState(multiValueChangeTimeLine.getMultiValueChangeEvents());
        origValue = indexDefinition.createValue(restoredMultiValue);
      } else
        origValue = indexDefinition.createValue(iRecord.getOriginalValue(indexField));

      deleteIndexKey(index, iRecord, origValue);
      return true;
    }
    return false;
  }

  private static ODocument checkForLoading(final ODocument iRecord) {
    if (iRecord.getInternalStatus() == ORecordElement.STATUS.NOT_LOADED) {
      try {
        return (ODocument) iRecord.load();
      } catch (final ORecordNotFoundException e) {
        throw OException.wrapException(new OIndexException("Error during loading of record with id " + iRecord.getIdentity()), e);
      }
    }
    return iRecord;
  }

  private void processIndexUpdate(ODocument iDocument, Set<String> dirtyFields, OIndex<?> index) {
    if (index.getDefinition() instanceof OCompositeIndexDefinition)
      processCompositeIndexUpdate(index, dirtyFields, iDocument);
    else
      processSingleIndexUpdate(index, dirtyFields, iDocument);
  }

  private void addIndexesEntries(ODocument document, final Collection<OIndex<?>> indexes) {
    // STORE THE RECORD IF NEW, OTHERWISE ITS RID
    final OIdentifiable rid = document.getIdentity();

    for (final OIndex<?> index : indexes) {
      addIndexEntry(document, rid, index);
    }
  }

  private void addIndexEntry(ODocument document, OIdentifiable rid, OIndex<?> index) {
    final OIndexDefinition indexDefinition = index.getDefinition();
    final Object key = indexDefinition.getDocumentValueToIndex(document);
    if (key instanceof Collection) {
      for (final Object keyItem : (Collection<?>) key)
        if (!indexDefinition.isNullValuesIgnored() || keyItem != null)
          putInIndex(index, keyItem, rid);
    } else if (!indexDefinition.isNullValuesIgnored() || key != null)
      putInIndex(index, key, rid);
  }

  private void deleteIndexEntries(ODocument iDocument) {
    final OClass cls = ODocumentInternal.getImmutableSchemaClass(iDocument);
    if (cls == null)
      return;

    final Collection<OIndex<?>> indexes = new ArrayList<>(cls.getIndexes());

    if (!indexes.isEmpty()) {
      final Set<String> dirtyFields = new HashSet<>(Arrays.asList(iDocument.getDirtyFields()));

      if (!dirtyFields.isEmpty()) {
        // REMOVE INDEX OF ENTRIES FOR THE OLD VALUES
        final Iterator<OIndex<?>> indexIterator = indexes.iterator();

        while (indexIterator.hasNext()) {
          final OIndex<?> index = indexIterator.next();

          final boolean result;
          if (index.getDefinition() instanceof OCompositeIndexDefinition)
            result = processCompositeIndexDelete(index, dirtyFields, iDocument);
          else
            result = processSingleIndexDelete(index, dirtyFields, iDocument);

          if (result)
            indexIterator.remove();
        }
      }

      // REMOVE INDEX OF ENTRIES FOR THE NON CHANGED ONLY VALUES
      for (final OIndex<?> index : indexes) {
        final Object key = index.getDefinition().getDocumentValueToIndex(iDocument);
        deleteIndexKey(index, iDocument, key);
      }
    }
  }

}
