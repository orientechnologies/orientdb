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
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeTimeLine;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.OTrackedMultiValue;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Handles indexing when records change.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com), Artem Orobets
 */
public class OClassIndexManager {

  public static class IndexChange {
    public final OIndex index;
    public final OTransactionIndexChanges.OPERATION operation;
    public final Object key;
    public final OIdentifiable value;

    public IndexChange(
        OIndex indexName,
        OTransactionIndexChanges.OPERATION operation,
        Object key,
        OIdentifiable value) {
      this.index = indexName;
      this.operation = operation;
      this.key = key;
      this.value = value;
    }
  }

  public static void checkIndexesAfterCreate(
      ODocument document, ODatabaseDocumentInternal database) {
    document = checkForLoading(document);
    List<IndexChange> ops = new ArrayList<>();

    processIndexOnCreate(database, document, ops);
    applyChanges(ops);
  }

  public static void reIndex(ODocument document, ODatabaseDocumentInternal database, OIndex index) {
    document = checkForLoading(document);
    List<IndexChange> ops = new ArrayList<>();
    addIndexEntry(document, document.getIdentity(), getTransactionalIndex(database, index), ops);
    applyChanges(ops);
  }

  public static void processIndexOnCreate(
      ODatabaseDocumentInternal database, ODocument document, List<IndexChange> ops) {
    final OImmutableClass cls = ODocumentInternal.getImmutableSchemaClass(database, document);
    if (cls != null) {
      final Collection<OIndex> indexes = cls.getRawIndexes();
      addIndexesEntries(database, document, indexes, ops);
    }
  }

  public static void checkIndexesAfterUpdate(
      ODocument iDocument, ODatabaseDocumentInternal database) {
    iDocument = checkForLoading(iDocument);
    List<IndexChange> changes = new ArrayList<>();
    processIndexOnUpdate(database, iDocument, changes);
    applyChanges(changes);
  }

  public static void processIndexOnUpdate(
      ODatabaseDocumentInternal database, ODocument iDocument, List<IndexChange> changes) {
    final OImmutableClass cls = ODocumentInternal.getImmutableSchemaClass(database, iDocument);
    if (cls == null) {
      return;
    }

    final Collection<OIndex> indexes = cls.getRawIndexes();
    if (!indexes.isEmpty()) {
      final Set<String> dirtyFields = new HashSet<>(Arrays.asList(iDocument.getDirtyFields()));
      if (!dirtyFields.isEmpty())
        for (final OIndex index : indexes) {
          processIndexUpdate(
              iDocument, dirtyFields, getTransactionalIndex(database, index), changes);
        }
    }
  }

  private static OIndex getTransactionalIndex(ODatabaseDocumentInternal database, OIndex index) {
    return index;
  }

  public static void checkIndexesAfterDelete(
      ODocument iDocument, ODatabaseDocumentInternal database) {
    List<IndexChange> changes = new ArrayList<>();
    processIndexOnDelete(database, iDocument, changes);
    applyChanges(changes);
  }

  protected static void putInIndex(OIndex index, Object key, OIdentifiable value) {
    index.put(key, value);
  }

  protected static void removeFromIndex(OIndex index, Object key, OIdentifiable value) {
    index.remove(key, value);
  }

  private static void processCompositeIndexUpdate(
      final OIndex index,
      final Set<String> dirtyFields,
      final ODocument iRecord,
      List<IndexChange> changes) {
    final OCompositeIndexDefinition indexDefinition =
        (OCompositeIndexDefinition) index.getDefinition();

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
            addRemove(changes, index, origValue, iRecord);

          if (!indexDefinition.isNullValuesIgnored() || newValue != null)
            addPut(changes, index, newValue, iRecord.getIdentity());
        } else {
          final OMultiValueChangeTimeLine<?, ?> multiValueChangeTimeLine =
              iRecord.getCollectionTimeLine(multiValueField);
          if (multiValueChangeTimeLine == null) {
            if (dirtyFields.contains(multiValueField))
              origValues.add(
                  indexDefinition.getMultiValueDefinitionIndex(),
                  iRecord.getOriginalValue(multiValueField));
            else
              origValues.add(
                  indexDefinition.getMultiValueDefinitionIndex(), iRecord.field(multiValueField));

            final Object origValue = indexDefinition.createValue(origValues);
            final Object newValue = indexDefinition.getDocumentValueToIndex(iRecord);

            processIndexUpdateFieldAssignment(index, iRecord, origValue, newValue, changes);
          } else {
            // in case of null values support and empty collection field we put null placeholder in
            // place where collection item should be located so we can not use "fast path" to
            // update index values
            if (dirtyFields.size() == 1 && indexDefinition.isNullValuesIgnored()) {
              final Map<OCompositeKey, Integer> keysToAdd = new HashMap<>();
              final Map<OCompositeKey, Integer> keysToRemove = new HashMap<>();

              for (OMultiValueChangeEvent<?, ?> changeEvent :
                  multiValueChangeTimeLine.getMultiValueChangeEvents()) {
                indexDefinition.processChangeEvent(
                    changeEvent, keysToAdd, keysToRemove, origValues.toArray());
              }

              for (final Object keyToRemove : keysToRemove.keySet())
                addRemove(changes, index, keyToRemove, iRecord);

              for (final Object keyToAdd : keysToAdd.keySet())
                addPut(changes, index, keyToAdd, iRecord.getIdentity());
            } else {
              final OTrackedMultiValue fieldValue = iRecord.field(multiValueField);
              @SuppressWarnings("unchecked")
              final Object restoredMultiValue =
                  fieldValue.returnOriginalState(
                      multiValueChangeTimeLine.getMultiValueChangeEvents());

              origValues.add(indexDefinition.getMultiValueDefinitionIndex(), restoredMultiValue);

              final Object origValue = indexDefinition.createValue(origValues);
              final Object newValue = indexDefinition.getDocumentValueToIndex(iRecord);

              processIndexUpdateFieldAssignment(index, iRecord, origValue, newValue, changes);
            }
          }
        }
        return;
      }
    }
    return;
  }

  private static void processSingleIndexUpdate(
      final OIndex index,
      final Set<String> dirtyFields,
      final ODocument iRecord,
      List<IndexChange> changes) {
    final OIndexDefinition indexDefinition = index.getDefinition();
    final List<String> indexFields = indexDefinition.getFields();

    if (indexFields.isEmpty()) return;

    final String indexField = indexFields.get(0);
    if (!dirtyFields.contains(indexField)) return;

    final OMultiValueChangeTimeLine<?, ?> multiValueChangeTimeLine =
        iRecord.getCollectionTimeLine(indexField);
    if (multiValueChangeTimeLine != null) {
      final OIndexDefinitionMultiValue indexDefinitionMultiValue =
          (OIndexDefinitionMultiValue) indexDefinition;
      final Map<Object, Integer> keysToAdd = new HashMap<>();
      final Map<Object, Integer> keysToRemove = new HashMap<>();

      for (OMultiValueChangeEvent<?, ?> changeEvent :
          multiValueChangeTimeLine.getMultiValueChangeEvents()) {
        indexDefinitionMultiValue.processChangeEvent(changeEvent, keysToAdd, keysToRemove);
      }

      for (final Object keyToRemove : keysToRemove.keySet())
        addRemove(changes, index, keyToRemove, iRecord);

      for (final Object keyToAdd : keysToAdd.keySet())
        addPut(changes, index, keyToAdd, iRecord.getIdentity());

    } else {
      final Object origValue = indexDefinition.createValue(iRecord.getOriginalValue(indexField));
      final Object newValue = indexDefinition.getDocumentValueToIndex(iRecord);

      processIndexUpdateFieldAssignment(index, iRecord, origValue, newValue, changes);
    }
  }

  private static void processIndexUpdateFieldAssignment(
      OIndex index,
      ODocument iRecord,
      final Object origValue,
      final Object newValue,
      List<IndexChange> changes) {
    final OIndexDefinition indexDefinition = index.getDefinition();
    if ((origValue instanceof Collection) && (newValue instanceof Collection)) {
      final Set<Object> valuesToRemove = new HashSet<>((Collection<?>) origValue);
      final Set<Object> valuesToAdd = new HashSet<>((Collection<?>) newValue);

      valuesToRemove.removeAll((Collection<?>) newValue);
      valuesToAdd.removeAll((Collection<?>) origValue);

      for (final Object valueToRemove : valuesToRemove) {
        if (!indexDefinition.isNullValuesIgnored() || valueToRemove != null) {
          addRemove(changes, index, valueToRemove, iRecord);
        }
      }

      for (final Object valueToAdd : valuesToAdd) {
        if (!indexDefinition.isNullValuesIgnored() || valueToAdd != null) {
          addPut(changes, index, valueToAdd, iRecord);
        }
      }
    } else {
      deleteIndexKey(index, iRecord, origValue, changes);

      if (newValue instanceof Collection) {
        for (final Object newValueItem : (Collection<?>) newValue) {
          addPut(changes, index, newValueItem, iRecord.getIdentity());
        }
      } else if (!indexDefinition.isNullValuesIgnored() || newValue != null) {
        addPut(changes, index, newValue, iRecord.getIdentity());
      }
    }
  }

  private static boolean processCompositeIndexDelete(
      final OIndex index,
      final Set<String> dirtyFields,
      final ODocument iRecord,
      List<IndexChange> changes) {
    final OCompositeIndexDefinition indexDefinition =
        (OCompositeIndexDefinition) index.getDefinition();

    final String multiValueField = indexDefinition.getMultiValueField();

    final List<String> indexFields = indexDefinition.getFields();
    for (final String indexField : indexFields) {
      // REMOVE IT
      if (dirtyFields.contains(indexField)) {
        final List<Object> origValues = new ArrayList<>(indexFields.size());

        for (final String field : indexFields) {
          if (!field.equals(multiValueField))
            if (dirtyFields.contains(field)) {
              origValues.add(iRecord.getOriginalValue(field));
            } else origValues.add(iRecord.field(field));
        }

        if (multiValueField != null) {
          final OMultiValueChangeTimeLine<?, ?> multiValueChangeTimeLine =
              iRecord.getCollectionTimeLine(multiValueField);
          if (multiValueChangeTimeLine != null) {
            final OTrackedMultiValue fieldValue = iRecord.field(multiValueField);
            @SuppressWarnings("unchecked")
            final Object restoredMultiValue =
                fieldValue.returnOriginalState(
                    multiValueChangeTimeLine.getMultiValueChangeEvents());
            origValues.add(indexDefinition.getMultiValueDefinitionIndex(), restoredMultiValue);
          } else if (dirtyFields.contains(multiValueField))
            origValues.add(
                indexDefinition.getMultiValueDefinitionIndex(),
                iRecord.getOriginalValue(multiValueField));
          else
            origValues.add(
                indexDefinition.getMultiValueDefinitionIndex(), iRecord.field(multiValueField));
        }

        final Object origValue = indexDefinition.createValue(origValues);
        deleteIndexKey(index, iRecord, origValue, changes);
        return true;
      }
    }
    return false;
  }

  private static void deleteIndexKey(
      final OIndex index,
      final ODocument iRecord,
      final Object origValue,
      List<IndexChange> changes) {
    final OIndexDefinition indexDefinition = index.getDefinition();
    if (origValue instanceof Collection) {
      for (final Object valueItem : (Collection<?>) origValue) {
        if (!indexDefinition.isNullValuesIgnored() || valueItem != null)
          addRemove(changes, index, valueItem, iRecord);
      }
    } else if (!indexDefinition.isNullValuesIgnored() || origValue != null) {
      addRemove(changes, index, origValue, iRecord);
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static boolean processSingleIndexDelete(
      final OIndex index,
      final Set<String> dirtyFields,
      final ODocument iRecord,
      List<IndexChange> changes) {
    final OIndexDefinition indexDefinition = index.getDefinition();

    final List<String> indexFields = indexDefinition.getFields();
    if (indexFields.isEmpty()) {
      return false;
    }

    final String indexField = indexFields.iterator().next();
    if (dirtyFields.contains(indexField)) {
      final OMultiValueChangeTimeLine<?, ?> multiValueChangeTimeLine =
          iRecord.getCollectionTimeLine(indexField);

      final Object origValue;
      if (multiValueChangeTimeLine != null) {
        final OTrackedMultiValue fieldValue = iRecord.field(indexField);
        final Object restoredMultiValue =
            fieldValue.returnOriginalState(multiValueChangeTimeLine.getMultiValueChangeEvents());
        origValue = indexDefinition.createValue(restoredMultiValue);
      } else origValue = indexDefinition.createValue(iRecord.getOriginalValue(indexField));
      deleteIndexKey(index, iRecord, origValue, changes);
      return true;
    }
    return false;
  }

  private static ODocument checkForLoading(final ODocument iRecord) {
    if (iRecord.getInternalStatus() == ORecordElement.STATUS.NOT_LOADED) {
      try {
        return (ODocument) iRecord.load();
      } catch (final ORecordNotFoundException e) {
        throw OException.wrapException(
            new OIndexException("Error during loading of record with id " + iRecord.getIdentity()),
            e);
      }
    }
    return iRecord;
  }

  public static void processIndexUpdate(
      ODocument iDocument, Set<String> dirtyFields, OIndex index, List<IndexChange> changes) {
    if (index.getDefinition() instanceof OCompositeIndexDefinition)
      processCompositeIndexUpdate(index, dirtyFields, iDocument, changes);
    else processSingleIndexUpdate(index, dirtyFields, iDocument, changes);
  }

  public static void addIndexesEntries(
      ODatabaseDocumentInternal database,
      ODocument document,
      final Collection<OIndex> indexes,
      List<IndexChange> changes) {
    // STORE THE RECORD IF NEW, OTHERWISE ITS RID
    final OIdentifiable rid = document.getIdentity();

    for (final OIndex index : indexes) {
      addIndexEntry(document, rid, getTransactionalIndex(database, index), changes);
    }
  }

  private static void addIndexEntry(
      ODocument document, OIdentifiable rid, OIndex index, List<IndexChange> changes) {
    final OIndexDefinition indexDefinition = index.getDefinition();
    final Object key = indexDefinition.getDocumentValueToIndex(document);
    if (key instanceof Collection) {
      for (final Object keyItem : (Collection<?>) key) {
        if (!indexDefinition.isNullValuesIgnored() || keyItem != null) {
          addPut(changes, index, keyItem, rid);
        }
      }
    } else if (!indexDefinition.isNullValuesIgnored() || key != null)
      addPut(changes, index, key, rid);
  }

  public static void processIndexOnDelete(
      ODatabaseDocumentInternal database, ODocument iDocument, List<IndexChange> changes) {
    final OImmutableClass cls = ODocumentInternal.getImmutableSchemaClass(database, iDocument);
    if (cls == null) return;

    final Collection<OIndex> indexes = new ArrayList<>();
    for (OIndex index : cls.getRawIndexes()) {
      indexes.add(getTransactionalIndex(database, index));
    }

    if (!indexes.isEmpty()) {
      final Set<String> dirtyFields = new HashSet<>(Arrays.asList(iDocument.getDirtyFields()));

      if (!dirtyFields.isEmpty()) {
        // REMOVE INDEX OF ENTRIES FOR THE OLD VALUES
        final Iterator<OIndex> indexIterator = indexes.iterator();

        while (indexIterator.hasNext()) {
          final OIndex index = indexIterator.next();

          final boolean result;
          if (index.getDefinition() instanceof OCompositeIndexDefinition)
            result = processCompositeIndexDelete(index, dirtyFields, iDocument, changes);
          else result = processSingleIndexDelete(index, dirtyFields, iDocument, changes);

          if (result) indexIterator.remove();
        }
      }
    }

    // REMOVE INDEX OF ENTRIES FOR THE NON CHANGED ONLY VALUES
    for (final OIndex index : indexes) {
      final Object key = index.getDefinition().getDocumentValueToIndex(iDocument);
      deleteIndexKey(index, iDocument, key, changes);
    }
  }

  private static void addPut(
      List<IndexChange> changes, OIndex index, Object key, OIdentifiable value) {
    changes.add(new IndexChange(index, OTransactionIndexChanges.OPERATION.PUT, key, value));
  }

  private static void addRemove(
      List<IndexChange> changes, OIndex index, Object key, OIdentifiable value) {
    changes.add(new IndexChange(index, OTransactionIndexChanges.OPERATION.REMOVE, key, value));
  }

  public static void applyChanges(List<IndexChange> changes) {
    for (IndexChange op : changes) {
      if (op.operation == OTransactionIndexChanges.OPERATION.PUT) {
        putInIndex(op.index, op.key, op.value);
      } else {
        removeFromIndex(op.index, op.key, op.value);
      }
    }
  }
}
