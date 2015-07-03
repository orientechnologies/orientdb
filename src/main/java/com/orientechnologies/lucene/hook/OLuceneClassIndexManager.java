/*
 * Copyright 2014 Orient Technologies.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.lucene.hook;

import static com.orientechnologies.orient.core.hook.ORecordHook.TYPE.BEFORE_CREATE;
import static com.orientechnologies.orient.core.hook.ORecordHook.TYPE.BEFORE_UPDATE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.lucene.OLuceneIndex;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeTimeLine;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.record.OTrackedMultiValue;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OFastConcurrentModificationException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexDefinitionMultiValue;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.version.ORecordVersion;

public class OLuceneClassIndexManager extends ODocumentHookAbstract {

  @Override
  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.TARGET_NODE;
  }

  @Override
  public RESULT onRecordBeforeCreate(ODocument iDocument) {
    checkIndexes(iDocument, BEFORE_CREATE);
    return RESULT.RECORD_NOT_CHANGED;
  }

  @Override
  public void onRecordAfterCreate(ODocument iDocument) {
    addIndexesEntries(iDocument);
  }

  private void addIndexesEntries(ODocument document) {
    document = checkForLoading(document);

    // STORE THE RECORD IF NEW, OTHERWISE ITS RID
    final OIdentifiable rid = document.getIdentity().isPersistent() ? document.placeholder() : document;

    final OClass cls = document.getSchemaClass();
    if (cls != null) {
      final Collection<OIndex<?>> indexes = cls.getIndexes();
      for (final OIndex<?> index : indexes) {
        final Object key = index.getDefinition().getDocumentValueToIndex(document);
        // SAVE A COPY TO AVOID PROBLEM ON RECYCLING OF THE RECORD
        if (key instanceof Collection) {
          for (final Object keyItem : (Collection<?>) key)
            if (keyItem != null)
              index.put(keyItem, rid);
        } else if (key != null)
          index.put(key, rid);
      }

    }
  }

  @Override
  public void onRecordCreateFailed(ODocument iDocument) {
  }

  @Override
  public void onRecordCreateReplicated(ODocument iDocument) {
  }

  @Override
  public RESULT onRecordBeforeUpdate(ODocument iDocument) {
    checkIndexes(iDocument, BEFORE_UPDATE);
    return RESULT.RECORD_NOT_CHANGED;
  }

  @Override
  public void onRecordAfterUpdate(ODocument iDocument) {
    updateIndexEntries(iDocument);
  }

  private void updateIndexEntries(ODocument iDocument) {
    iDocument = checkForLoading(iDocument);

    final OClass cls = iDocument.getSchemaClass();
    if (cls == null)
      return;

    final Collection<OIndex<?>> indexes = (Collection<OIndex<?>>) getDatabase().getMetadata().getIndexManager().getIndexes();

    if (!indexes.isEmpty()) {
      final Set<String> dirtyFields = new HashSet<String>(Arrays.asList(iDocument.getDirtyFields()));
      if (!dirtyFields.isEmpty()) {
        for (final OIndex<?> index : indexes) {
          if (index.getInternal() instanceof OLuceneIndex && index.getConfiguration().field("metadata") != null) {
            if (index.getDefinition() instanceof OCompositeIndexDefinition)
              processCompositeIndexUpdate(index, dirtyFields, iDocument);
            else
              processSingleIndexUpdate(index, dirtyFields, iDocument);

            if (iDocument.isTrackingChanges()) {
              iDocument.setTrackingChanges(false);
              iDocument.setTrackingChanges(true);
            }
          }
        }
      }
    }

  }

  @Override
  public void onRecordUpdateFailed(ODocument iDocument) {
  }

  @Override
  public void onRecordUpdateReplicated(ODocument iDocument) {
  }

  @Override
  public RESULT onRecordBeforeDelete(final ODocument iDocument) {
    final ORecordVersion version = iDocument.getRecordVersion(); // Cache the transaction-provided value
    if (iDocument.fields() == 0) {
      // FORCE LOADING OF CLASS+FIELDS TO USE IT AFTER ON onRecordAfterDelete METHOD
      iDocument.reload();
      if (version.getCounter() > -1 && iDocument.getRecordVersion().compareTo(version) != 0) // check for record version errors
        if (OFastConcurrentModificationException.enabled())
          throw OFastConcurrentModificationException.instance();
        else
          throw new OConcurrentModificationException(iDocument.getIdentity(), iDocument.getRecordVersion(), version,
              ORecordOperation.DELETED);
    }

    return RESULT.RECORD_NOT_CHANGED;
  }

  @Override
  public void onRecordAfterDelete(final ODocument iDocument) {
    deleteIndexEntries(iDocument);
  }

  private void deleteIndexEntries(ODocument iDocument) {
    final OClass cls = iDocument.getSchemaClass();
    if (cls == null)
      return;

    final Collection<OIndex<?>> indexes = new ArrayList<OIndex<?>>(cls.getIndexes());

    if (!indexes.isEmpty()) {
      final Set<String> dirtyFields = new HashSet<String>(Arrays.asList(iDocument.getDirtyFields()));

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

    if (iDocument.isTrackingChanges()) {
      iDocument.setTrackingChanges(false);
      iDocument.setTrackingChanges(true);
    }
  }

  @Override
  public void onRecordDeleteFailed(ODocument iDocument) {
  }

  @Override
  public void onRecordDeleteReplicated(ODocument iDocument) {
  }

  private static void processCompositeIndexUpdate(final OIndex<?> index, final Set<String> dirtyFields, final ODocument iRecord) {
    final OCompositeIndexDefinition indexDefinition = (OCompositeIndexDefinition) index.getDefinition();

    final List<String> indexFields = indexDefinition.getFields();
    final String multiValueField = indexDefinition.getMultiValueField();

    for (final String indexField : indexFields) {
      if (dirtyFields.contains(indexField)) {
        final List<Object> origValues = new ArrayList<Object>(indexFields.size());

        for (final String field : indexFields) {
          if (!field.equals(multiValueField))
            if (dirtyFields.contains(field)) {
              origValues.add(iRecord.getOriginalValue(field));
            } else {
              origValues.add(iRecord.<Object> field(field));
            }
        }

        if (multiValueField == null) {
          final Object origValue = indexDefinition.createValue(origValues);
          final Object newValue = indexDefinition.getDocumentValueToIndex(iRecord);

          if (origValue != null)
            index.remove(origValue, iRecord);

          if (newValue != null)
            index.put(newValue, iRecord.placeholder());
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
            if (dirtyFields.size() == 1) {
              final Map<OCompositeKey, Integer> keysToAdd = new HashMap<OCompositeKey, Integer>();
              final Map<OCompositeKey, Integer> keysToRemove = new HashMap<OCompositeKey, Integer>();

              for (OMultiValueChangeEvent<?, ?> changeEvent : multiValueChangeTimeLine.getMultiValueChangeEvents()) {
                indexDefinition.processChangeEvent(changeEvent, keysToAdd, keysToRemove, origValues.toArray());
              }

              for (final Object keyToRemove : keysToRemove.keySet())
                index.remove(keyToRemove, iRecord);

              for (final Object keyToAdd : keysToAdd.keySet())
                index.put(keyToAdd, iRecord.placeholder());
            } else {
              final OTrackedMultiValue fieldValue = iRecord.field(multiValueField);
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

  private static void processSingleIndexUpdate(final OIndex<?> index, final Set<String> dirtyFields, final ODocument iRecord) {
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
      final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
      final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();

      for (OMultiValueChangeEvent<?, ?> changeEvent : multiValueChangeTimeLine.getMultiValueChangeEvents()) {
        indexDefinitionMultiValue.processChangeEvent(changeEvent, keysToAdd, keysToRemove);
      }

      for (final Object keyToRemove : keysToRemove.keySet())
        index.remove(keyToRemove, iRecord);

      for (final Object keyToAdd : keysToAdd.keySet())
        index.put(keyToAdd, iRecord.placeholder());

    } else {
      final Object origValue = indexDefinition.createValue(iRecord.getOriginalValue(indexField));
      final Object newValue = indexDefinition.getDocumentValueToIndex(iRecord);

      processIndexUpdateFieldAssignment(index, iRecord, origValue, newValue);
    }
  }

  private static void processIndexUpdateFieldAssignment(OIndex<?> index, ODocument iRecord, final Object origValue,
      final Object newValue) {
    if ((origValue instanceof Collection) && (newValue instanceof Collection)) {
      final Set<Object> valuesToRemove = new HashSet<Object>((Collection<?>) origValue);
      final Set<Object> valuesToAdd = new HashSet<Object>((Collection<?>) newValue);

      valuesToRemove.removeAll((Collection<?>) newValue);
      valuesToAdd.removeAll((Collection<?>) origValue);

      for (final Object valueToRemove : valuesToRemove) {
        if (valueToRemove != null) {
          index.remove(valueToRemove, iRecord);
        }
      }

      for (final Object valueToAdd : valuesToAdd) {
        if (valueToAdd != null) {
          index.put(valueToAdd, iRecord);
        }
      }
    } else {
      deleteIndexKey(index, iRecord, origValue);

      if (newValue instanceof Collection) {
        for (final Object newValueItem : (Collection<?>) newValue) {
          index.put(newValueItem, iRecord.placeholder());
        }
      } else if (newValue != null) {
        index.put(newValue, iRecord.placeholder());
      }
    }
  }

  private static boolean processCompositeIndexDelete(final OIndex<?> index, final Set<String> dirtyFields, final ODocument iRecord) {
    final OCompositeIndexDefinition indexDefinition = (OCompositeIndexDefinition) index.getDefinition();

    final String multiValueField = indexDefinition.getMultiValueField();

    final List<String> indexFields = indexDefinition.getFields();
    for (final String indexField : indexFields) {
      // REMOVE IT
      if (dirtyFields.contains(indexField)) {
        final List<Object> origValues = new ArrayList<Object>(indexFields.size());

        for (final String field : indexFields) {
          if (!field.equals(multiValueField))
            if (dirtyFields.contains(field))
              origValues.add(iRecord.getOriginalValue(field));
            else
              origValues.add(iRecord.<Object> field(field));
        }

        if (multiValueField != null) {
          final OMultiValueChangeTimeLine<?, ?> multiValueChangeTimeLine = iRecord.getCollectionTimeLine(multiValueField);
          if (multiValueChangeTimeLine != null) {
            final OTrackedMultiValue fieldValue = iRecord.field(multiValueField);
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

  private static void deleteIndexKey(final OIndex<?> index, final ODocument iRecord, final Object origValue) {
    if (origValue instanceof Collection) {
      for (final Object valueItem : (Collection<?>) origValue) {
        if (valueItem != null)
          index.remove(valueItem, iRecord);
      }
    } else if (origValue != null) {
      index.remove(origValue, iRecord);
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private static boolean processSingleIndexDelete(final OIndex<?> index, final Set<String> dirtyFields, final ODocument iRecord) {
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

  private void checkIndexes(ODocument document, TYPE hookType) {
    document = checkForLoading(document);

    final OClass cls = document.getSchemaClass();
    if (cls != null) {
      final Collection<OIndex<?>> indexes = cls.getIndexes();
      switch (hookType) {
      case BEFORE_CREATE:
        checkIndexedPropertiesOnCreation(document, indexes);
        break;
      case BEFORE_UPDATE:
        checkIndexedPropertiesOnUpdate(document, indexes);
        break;
      default:
        throw new IllegalArgumentException("Invalid hook type: " + hookType);
      }
    }
  }

  private static void checkIndexedPropertiesOnCreation(final ODocument iRecord, final Collection<OIndex<?>> iIndexes) {
    for (final OIndex<?> index : iIndexes) {
      final Object key = index.getDefinition().getDocumentValueToIndex(iRecord);
      if (key instanceof Collection) {
        for (final Object keyItem : (Collection<?>) key) {
          if (keyItem != null)
            index.checkEntry(iRecord, keyItem);
        }
      } else {
        if (key != null)
          index.checkEntry(iRecord, key);
      }
    }
  }

  private static void checkIndexedPropertiesOnUpdate(final ODocument iRecord, final Collection<OIndex<?>> iIndexes) {
    final Set<String> dirtyFields = new HashSet<String>(Arrays.asList(iRecord.getDirtyFields()));
    if (dirtyFields.isEmpty())
      return;

    for (final OIndex<?> index : iIndexes) {
      final OIndexDefinition indexDefinition = index.getDefinition();
      final List<String> indexFields = indexDefinition.getFields();
      for (final String indexField : indexFields) {
        if (dirtyFields.contains(indexField)) {
          final Object key = index.getDefinition().getDocumentValueToIndex(iRecord);
          if (key instanceof Collection) {
            for (final Object keyItem : (Collection<?>) key) {
              if (keyItem != null)
                index.checkEntry(iRecord, keyItem);
            }
          } else {
            if (key != null)
              index.checkEntry(iRecord, key);
          }
          break;
        }
      }
    }
  }

  private static ODocument checkForLoading(final ODocument iRecord) {
    if (iRecord.getInternalStatus() == ORecordElement.STATUS.NOT_LOADED) {
      try {
        return (ODocument) iRecord.load();
      } catch (final ORecordNotFoundException e) {
        throw new OIndexException("Error during loading of record with id : " + iRecord.getIdentity());
      }
    }
    return iRecord;
  }

  private ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }
}
