/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.orient.core.db.OHookReplacedRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.*;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OFastConcurrentModificationException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;

import java.io.IOException;
import java.util.*;

import static com.orientechnologies.orient.core.hook.ORecordHook.TYPE.BEFORE_CREATE;
import static com.orientechnologies.orient.core.hook.ORecordHook.TYPE.BEFORE_UPDATE;

/**
 * Handles indexing when records change.
 *
 * @author Andrey Lomakin, Artem Orobets - initial contribution
 * @author Sergey Sitnikov – atomic operations support
 */
public class OClassIndexManager extends ODocumentHookAbstract {
  private final ThreadLocal<Boolean> threadAtomicOperation = new ThreadLocal<Boolean>();

  public OClassIndexManager(ODatabaseDocument database) {
    super(database);
  }

  private void processCompositeIndexUpdate(final OIndex<?> index, final Set<String> dirtyFields, final ODocument iRecord) {
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
              origValues.add(iRecord.<Object>field(field));
            }
        }

        if (multiValueField == null) {
          final Object origValue = indexDefinition.createValue(origValues);
          final Object newValue = indexDefinition.getDocumentValueToIndex(iRecord);

          if (!indexDefinition.isNullValuesIgnored() || origValue != null)
            removeFromIndex(index,origValue,iRecord);

          if (!indexDefinition.isNullValuesIgnored() || newValue != null)
            putInIndex(index,newValue,iRecord.getIdentity());
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
              final Map<OCompositeKey, Integer> keysToAdd = new HashMap<OCompositeKey, Integer>();
              final Map<OCompositeKey, Integer> keysToRemove = new HashMap<OCompositeKey, Integer>();

              for (OMultiValueChangeEvent<?, ?> changeEvent : multiValueChangeTimeLine.getMultiValueChangeEvents()) {
                indexDefinition.processChangeEvent(changeEvent, keysToAdd, keysToRemove, origValues.toArray());
              }

              for (final Object keyToRemove : keysToRemove.keySet())
                removeFromIndex(index,keyToRemove,iRecord);

              for (final Object keyToAdd : keysToAdd.keySet())
                putInIndex(index,keyToAdd,iRecord.getIdentity());
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
      final Map<Object, Integer> keysToAdd = new HashMap<Object, Integer>();
      final Map<Object, Integer> keysToRemove = new HashMap<Object, Integer>();

      for (OMultiValueChangeEvent<?, ?> changeEvent : multiValueChangeTimeLine.getMultiValueChangeEvents()) {
        indexDefinitionMultiValue.processChangeEvent(changeEvent, keysToAdd, keysToRemove);
      }

      for (final Object keyToRemove : keysToRemove.keySet())
        removeFromIndex(index, keyToRemove, iRecord);

      for (final Object keyToAdd : keysToAdd.keySet())
        putInIndex(index,keyToAdd,iRecord.getIdentity());

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
      final Set<Object> valuesToRemove = new HashSet<Object>((Collection<?>) origValue);
      final Set<Object> valuesToAdd = new HashSet<Object>((Collection<?>) newValue);

      valuesToRemove.removeAll((Collection<?>) newValue);
      valuesToAdd.removeAll((Collection<?>) origValue);

      for (final Object valueToRemove : valuesToRemove) {
        if (!indexDefinition.isNullValuesIgnored() || valueToRemove != null) {
          removeFromIndex(index,valueToRemove,iRecord);
        }
      }

      for (final Object valueToAdd : valuesToAdd) {
        if (!indexDefinition.isNullValuesIgnored() || valueToAdd != null) {
          putInIndex(index,valueToAdd,iRecord);
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
        final List<Object> origValues = new ArrayList<Object>(indexFields.size());

        for (final String field : indexFields) {
          if (!field.equals(multiValueField))
            if (dirtyFields.contains(field))
              origValues.add(iRecord.getOriginalValue(field));
            else
              origValues.add(iRecord.<Object>field(field));
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

  private ODocument checkIndexedPropertiesOnCreation(final ODocument record, final Collection<OIndex<?>> indexes) {
    ODocument replaced = null;

    for (final OIndex<?> index : indexes) {
      if (!(index.getInternal() instanceof OIndexUnique))
        continue;

      final OIndexRecorder indexRecorder = new OIndexRecorder((OIndexUnique) index.getInternal());
      addIndexEntry(record, record.getIdentity(), indexRecorder);

      for (Object keyItem : indexRecorder.getAffectedKeys()) {
        final ODocument r = index.checkEntry(record, keyItem);
        if (r != null)
          if (replaced == null)
            replaced = r;
          else
            throw new OIndexException("Cannot merge record from multiple indexes. Use this strategy when you have only one index");
      }
    }

    return replaced;
  }

  private void checkIndexedPropertiesOnUpdate(final ODocument record, final Collection<OIndex<?>> indexes) {
    final Set<String> dirtyFields = new HashSet<String>(Arrays.asList(record.getDirtyFields()));
    if (dirtyFields.isEmpty())
      return;

    for (final OIndex<?> index : indexes) {
      if (!(index.getInternal() instanceof OIndexUnique))
        continue;

      final OIndexRecorder indexRecorder = new OIndexRecorder((OIndexUnique) index.getInternal());
      processIndexUpdate(record, dirtyFields, indexRecorder);

      for (Object keyItem : indexRecorder.getAffectedKeys())
        index.checkEntry(record, keyItem);
    }
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

  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.BOTH;
  }

  @Override
  public RESULT onRecordBeforeCreate(final ODocument document) {
    final ODocument replaced = checkIndexes(document, BEFORE_CREATE);
    if (replaced != null) {
      OHookReplacedRecordThreadLocal.INSTANCE.set(replaced);
      return RESULT.RECORD_REPLACED;
    }
    return RESULT.RECORD_NOT_CHANGED;
  }

  @Override
  public void onRecordAfterCreate(ODocument document) {
    document = checkForLoading(document);

    final OClass cls = ODocumentInternal.getImmutableSchemaClass(document);
    if (cls != null) {
      final Collection<OIndex<?>> indexes = cls.getIndexes();
      addIndexesEntries(document, indexes);
    }
    endSuccessfulAtomicOperationIfAny(document);
  }

  @Override
  public void onRecordCreateFailed(final ODocument iDocument) {
    endFailedAtomicOperationIfAny(iDocument);
  }

  @Override
  public void onRecordCreateReplicated(final ODocument iDocument) {
    endSuccessfulAtomicOperationIfAny(iDocument);
  }

  @Override
  public RESULT onRecordBeforeUpdate(final ODocument iDocument) {
    checkIndexes(iDocument, BEFORE_UPDATE);
    return RESULT.RECORD_NOT_CHANGED;
  }

  @Override
  public void onRecordAfterUpdate(ODocument iDocument) {
    iDocument = checkForLoading(iDocument);

    final OClass cls = ODocumentInternal.getImmutableSchemaClass(iDocument);
    if (cls == null)
      return;

    final Collection<OIndex<?>> indexes = cls.getIndexes();

    if (!indexes.isEmpty()) {
      final Set<String> dirtyFields = new HashSet<String>(Arrays.asList(iDocument.getDirtyFields()));

      if (!dirtyFields.isEmpty()) {
        for (final OIndex<?> index : indexes) {
          try {
            processIndexUpdate(iDocument, dirtyFields, index);
          } catch (ORecordDuplicatedException ex) {
            iDocument.undo();
            iDocument.setDirty();
            database.save(iDocument);
            throw ex;
          }
        }
      }
    }

    endSuccessfulAtomicOperationIfAny(iDocument);
  }

  private void processIndexUpdate(ODocument iDocument, Set<String> dirtyFields, OIndex<?> index) {
    if (index.getDefinition() instanceof OCompositeIndexDefinition)
      processCompositeIndexUpdate(index, dirtyFields, iDocument);
    else
      processSingleIndexUpdate(index, dirtyFields, iDocument);
  }

  @Override
  public void onRecordUpdateFailed(final ODocument iDocument) {
    endFailedAtomicOperationIfAny(iDocument);
  }

  @Override
  public void onRecordUpdateReplicated(final ODocument iDocument) {
    endSuccessfulAtomicOperationIfAny(iDocument);
  }

  @Override
  public RESULT onRecordBeforeDelete(final ODocument iDocument) {
    final OClass class_ = ODocumentInternal.getImmutableSchemaClass(iDocument);
    try {
      startAtomicOperationIfRequired(iDocument, class_ == null ? Collections.<OIndex<?>>emptyList() : class_.getIndexes());

      final int version = iDocument.getVersion(); // Cache the transaction-provided value
      if (iDocument.fields() == 0 && iDocument.getIdentity().isPersistent()) {
        // FORCE LOADING OF CLASS+FIELDS TO USE IT AFTER ON onRecordAfterDelete METHOD
        iDocument.reload();
        if (version > -1 && iDocument.getVersion() != version) // check for record version errors
          if (OFastConcurrentModificationException.enabled())
            throw OFastConcurrentModificationException.instance();
          else
            throw new OConcurrentModificationException(iDocument.getIdentity(), iDocument.getVersion(), version,
                ORecordOperation.DELETED);
      }
    } catch (RuntimeException e) {
      endFailedAtomicOperationIfAny(iDocument);
      throw e;
    }

    return RESULT.RECORD_NOT_CHANGED;
  }

  @Override
  public void onRecordAfterDelete(final ODocument iDocument) {
    deleteIndexEntries(iDocument);
    endSuccessfulAtomicOperationIfAny(iDocument);
  }

  @Override
  public void onRecordDeleteReplicated(final ODocument iDocument) {
    endSuccessfulAtomicOperationIfAny(iDocument);
  }

  @Override
  public void onRecordDeleteFailed(final ODocument iDocument) {
    endFailedAtomicOperationIfAny(iDocument);
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
      try {
        putInIndex(index, key, rid);
      } catch (ORecordDuplicatedException e) {
        if (!database.getTransaction().isActive()) {
          database.delete(document);
        }
        throw e;
      }
  }

  private void deleteIndexEntries(ODocument iDocument) {
    final OClass cls = ODocumentInternal.getImmutableSchemaClass(iDocument);
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
  }

  private ODocument checkIndexes(ODocument document, TYPE hookType) {
    document = checkForLoading(document);

    ODocument replaced = null;

    final OClass cls = ODocumentInternal.getImmutableSchemaClass(document);
    if (cls != null) {
      final Collection<OIndex<?>> indexes =
          hookType == BEFORE_UPDATE ? getAffectedIndexes(cls.getIndexes(), document.getDirtyFields()) : cls.getIndexes();
      try {
        startAtomicOperationIfRequired(document, indexes);

        switch (hookType) {
        case BEFORE_CREATE:
          replaced = checkIndexedPropertiesOnCreation(document, indexes);
          break;
        case BEFORE_UPDATE:
          checkIndexedPropertiesOnUpdate(document, indexes);
          break;
        default:
          throw new IllegalArgumentException("Invalid hook type: " + hookType);
        }
      } catch (RuntimeException e) {
        endFailedAtomicOperationIfAny(document);
        throw e;
      }
    } else
      startAtomicOperationIfRequired(document, Collections.<OIndex<?>>emptyList());

    return replaced;
  }

  @Override
  public void onRecordFinalizeUpdate(ODocument document) {
    // if atomic operation is still active at this point, this indicates an error
    endFailedAtomicOperationIfAny(document);
  }

  @Override
  public void onRecordFinalizeCreation(ODocument document) {
    // if atomic operation is still active at this point, this indicates an error
    endFailedAtomicOperationIfAny(document);
  }

  @Override
  public void onRecordFinalizeDelete(ODocument document) {
    // if atomic operation is still active at this point, this indicates an error
    endFailedAtomicOperationIfAny(document);
  }

  protected void putInIndex(OIndex<?> index, Object key, OIdentifiable value) {
    index.put(key, value);
  }

  protected void removeFromIndex(OIndex<?> index, Object key, OIdentifiable value) {
    index.remove(key, value);
  }

  private void startAtomicOperationIfRequired(ODocument document, Collection<OIndex<?>> indexes) {
    boolean atomicOperation =
        !indexes.isEmpty() && !document.getDatabase().getTransaction().isActive() && document.getDatabase().getStorage()
            .getUnderlying() instanceof OAbstractPaginatedStorage;
    final OAbstractPaginatedStorage storage = atomicOperation ?
        (OAbstractPaginatedStorage) document.getDatabase().getStorage().getUnderlying() :
        null;
    final OAtomicOperationsManager atomicOperationsManager = atomicOperation ? storage.getAtomicOperationsManager() : null;

    if (atomicOperation)
      try {
        atomicOperation = atomicOperationsManager.startAtomicOperation((String) null, true) != null;
      } catch (IOException e) {
        threadAtomicOperation.set(false);
        throw OException.wrapException(new OIOException("Failed to start atomic operation."), e);
      }

    threadAtomicOperation.set(atomicOperation);

    if (atomicOperation) {
      // lock cluster
      storage.getClusterById(document.getIdentity().getClusterId()).acquireAtomicExclusiveLock();

      // lock indexes
      final OIndexInternal[] sortedIndexes = new OIndexInternal[indexes.size()];
      int i = 0;
      for (OIndex<?> index : indexes)
        sortedIndexes[i++] = index.getInternal();
      Arrays.sort(sortedIndexes, OIndexInternal.ID_COMPARATOR);
      for (OIndexInternal<?> index : sortedIndexes)
        index.acquireAtomicExclusiveLock();
    }
  }

  private void endSuccessfulAtomicOperationIfAny(ODocument document) {
    final Boolean atomicOperation = threadAtomicOperation.get();
    if (atomicOperation != null && atomicOperation) {
      final OAbstractPaginatedStorage storage = (OAbstractPaginatedStorage) document.getDatabase().getStorage().getUnderlying();
      try {
        threadAtomicOperation.set(false);
        storage.getAtomicOperationsManager().endAtomicOperation(false, null);
      } catch (IOException e) {
        throw OException.wrapException(new OIOException("Failed to end atomic operation."), e);
      }
    }
  }

  private void endFailedAtomicOperationIfAny(ODocument document) {
    final Boolean atomicOperation = threadAtomicOperation.get();
    if (atomicOperation != null && atomicOperation) {
      final OAbstractPaginatedStorage storage = (OAbstractPaginatedStorage) document.getDatabase().getStorage().getUnderlying();
      try {
        threadAtomicOperation.set(false);
        storage.getAtomicOperationsManager().endAtomicOperation(true, null);
      } catch (IOException e) {
        throw OException.wrapException(new OIOException("Failed to end atomic operation."), e);
      }
    }
  }

  private Collection<OIndex<?>> getAffectedIndexes(Collection<OIndex<?>> indexes, String[] dirtyFields) {
    if (indexes.isEmpty())
      return indexes;
    if (dirtyFields.length == 0) // XXX: that is possible, at least in 2.2.3 era
      return Collections.emptyList();

    final List<OIndex<?>> affectedIndexes = new ArrayList<OIndex<?>>(indexes.size());
    final Set<String> fields = new HashSet<String>(Arrays.asList(dirtyFields));
    for (OIndex<?> index : indexes)
      if (!Collections.disjoint(fields, index.getDefinition().getFields()))
        affectedIndexes.add(index);
    return affectedIndexes;
  }
}
