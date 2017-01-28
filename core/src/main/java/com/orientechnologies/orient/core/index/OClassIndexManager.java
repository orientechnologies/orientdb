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
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.OOrientShutdownListener;
import com.orientechnologies.orient.core.OOrientStartupListener;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.OHookReplacedRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.*;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OFastConcurrentModificationException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;

import java.util.*;
import java.util.concurrent.locks.Lock;

import static com.orientechnologies.orient.core.hook.ORecordHook.TYPE.BEFORE_CREATE;
import static com.orientechnologies.orient.core.hook.ORecordHook.TYPE.BEFORE_UPDATE;

/**
 * Handles indexing when records change.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com), Artem Orobets
 */
public class OClassIndexManager extends ODocumentHookAbstract implements OOrientStartupListener, OOrientShutdownListener {
  private Deque<List<Lock[]>> lockedKeys = new ArrayDeque<List<Lock[]>>();

  public OClassIndexManager(ODatabaseDocument database) {
    super(database);

    Orient.instance().registerWeakOrientStartupListener(this);
    Orient.instance().registerWeakOrientShutdownListener(this);
  }

  @Override
  public SCOPE[] getScopes() {
    return new SCOPE[] { SCOPE.CREATE, SCOPE.UPDATE, SCOPE.DELETE };
  }

  @Override
  public void onShutdown() {
    lockedKeys = null;
  }

  @Override
  public void onStartup() {
    if (lockedKeys == null)
      lockedKeys = new ArrayDeque<List<Lock[]>>();
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
              final Map<OCompositeKey, Integer> keysToAdd = new HashMap<OCompositeKey, Integer>();
              final Map<OCompositeKey, Integer> keysToRemove = new HashMap<OCompositeKey, Integer>();

              for (OMultiValueChangeEvent<?, ?> changeEvent : multiValueChangeTimeLine.getMultiValueChangeEvents()) {
                indexDefinition.processChangeEvent(changeEvent, keysToAdd, keysToRemove, origValues.toArray());
              }

              for (final Object keyToRemove : keysToRemove.keySet())
                removeFromIndex(index, keyToRemove, iRecord);

              for (final Object keyToAdd : keysToAdd.keySet())
                putInIndex(index, keyToAdd, iRecord.getIdentity());
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
  
  // Check if field is embedded. For e.g. person.address.city
  private boolean isFieldEmbedded(String indexField) {
	  if(indexField.contains("."))
		  return true;
	  else
		  return false;
  }
	 
  // Get Field of the document which contains an embedded. For e.g.
  // If IndexField is person.address.city, then field is person
  // If IndexField is mobiles[home].mobileNumber, then field is mobiles
  private String getFieldFromEmbeddedField(String embeddedField) {
	  String field = null;
	  if(embeddedField.contains("."))
		  field = embeddedField.split("\\.")[0];
	  if(embeddedField.contains("["))
		  field = embeddedField.split("\\[")[0];
	  return field;  
   }
	 
  // Get embedded key from the field. For e.g.
  // If IndexField is person.address.city, then Embedded key is address.city
  // If IndexField is mobiles[home].mobileNumber, then Embedded key is home.mobileNumber
  private String getEmbeddedKeyFromEmbeddedField(String embeddedField) {
	 String embeddedKey = null;
	 String field = getFieldFromEmbeddedField(embeddedField);
	 char firstSeaparator = embeddedField.charAt(field.length());
	 if(firstSeaparator == '.'){
		 embeddedKey = embeddedField.substring(field.length() + 1);
	 } else if(firstSeaparator == '['){
		 embeddedKey = embeddedField.substring(field.length() + 1).replaceFirst("\\]", "");
	 }
	 return embeddedKey;
  }

  private void processSingleIndexUpdate(final OIndex<?> index, final Set<String> dirtyFields, final ODocument iRecord) {
    final OIndexDefinition indexDefinition = index.getDefinition();
    final List<String> indexFields = indexDefinition.getFields();

    if (indexFields.isEmpty())
      return;

    final String indexField = indexFields.get(0);
    if(isFieldEmbedded(indexField)) { // If index is on an embedded field update the embedded index
    	String fieldWithEmbeddedIndex = getFieldFromEmbeddedField(indexField);
    	if (!dirtyFields.contains(fieldWithEmbeddedIndex))
    		return;
    	
			Object originalDocumentEntry = iRecord.getOriginalValue(fieldWithEmbeddedIndex);
			Object originalValue = ODocumentHelper.getFieldValue(originalDocumentEntry,
					getEmbeddedKeyFromEmbeddedField(indexField));
			final Object origValue = indexDefinition.createValue(originalValue);
			final Object newValue = indexDefinition.getDocumentValueToIndex(iRecord);

        processIndexUpdateFieldAssignment(index, iRecord, origValue, newValue);
    } else {
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
            putInIndex(index, keyToAdd, iRecord.getIdentity());

        } else {
          final Object origValue = indexDefinition.createValue(iRecord.getOriginalValue(indexField));
          final Object newValue = indexDefinition.getDocumentValueToIndex(iRecord);

          processIndexUpdateFieldAssignment(index, iRecord, origValue, newValue);
        }
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

    final TreeMap<OIndex<?>, List<Object>> indexKeysMap = new TreeMap<OIndex<?>, List<Object>>();

    for (final OIndex<?> index : indexes) {
      if (index.getInternal() instanceof OIndexUnique) {
        OIndexRecorder indexRecorder = new OIndexRecorder((OIndexUnique) index.getInternal());

        addIndexEntry(record, record.getIdentity(), indexRecorder);
        indexKeysMap.put(index, indexRecorder.getAffectedKeys());
      }
    }

    if (noTx(record)) {
      final List<Lock[]> locks = new ArrayList<Lock[]>(indexKeysMap.size());

      for (Map.Entry<OIndex<?>, List<Object>> entry : indexKeysMap.entrySet()) {
        final OIndexInternal<?> index = entry.getKey().getInternal();
        locks.add(index.lockKeysForUpdate(entry.getValue()));
      }

      lockedKeys.push(locks);
    }

    for (Map.Entry<OIndex<?>, List<Object>> entry : indexKeysMap.entrySet()) {
      final OIndex<?> index = entry.getKey();

      for (Object keyItem : entry.getValue()) {
        final ODocument r = index.checkEntry(record, keyItem);
        if (r != null)
          if (replaced == null)
            replaced = r;
          else {
            throw new OIndexException("Cannot merge record from multiple indexes. Use this strategy when you have only one index");
          }
      }
    }

    return replaced;
  }

  private void checkIndexedPropertiesOnUpdate(final ODocument record, final Collection<OIndex<?>> indexes) {
    final TreeMap<OIndex<?>, List<Object>> indexKeysMap = new TreeMap<OIndex<?>, List<Object>>();

    final Set<String> dirtyFields = new HashSet<String>(Arrays.asList(record.getDirtyFields()));
    if (dirtyFields.isEmpty())
      return;

    for (final OIndex<?> index : indexes) {

      if (index.getInternal() instanceof OIndexUnique) {
        final OIndexRecorder indexRecorder = new OIndexRecorder((OIndexInternal<OIdentifiable>) index.getInternal());
        processIndexUpdate(record, dirtyFields, indexRecorder);

        indexKeysMap.put(index, indexRecorder.getAffectedKeys());
      }
    }

    if (noTx(record)) {
      final List<Lock[]> locks = new ArrayList<Lock[]>(indexKeysMap.size());

      for (Map.Entry<OIndex<?>, List<Object>> entry : indexKeysMap.entrySet()) {
        final OIndexInternal<?> index = entry.getKey().getInternal();
        locks.add(index.lockKeysForUpdate(entry.getValue()));
      }

      lockedKeys.push(locks);
    }

    for (Map.Entry<OIndex<?>, List<Object>> entry : indexKeysMap.entrySet()) {
      final OIndex<?> index = entry.getKey();

      for (Object keyItem : entry.getValue()) {
        index.checkEntry(record, keyItem);
      }
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
  }

  @Override
  public void onRecordCreateFailed(final ODocument iDocument) {
  }

  @Override
  public void onRecordCreateReplicated(final ODocument iDocument) {
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
  }

  private void processIndexUpdate(ODocument iDocument, Set<String> dirtyFields, OIndex<?> index) {
    if (index.getDefinition() instanceof OCompositeIndexDefinition)
      processCompositeIndexUpdate(index, dirtyFields, iDocument);
    else
      processSingleIndexUpdate(index, dirtyFields, iDocument);
  }

  @Override
  public void onRecordUpdateFailed(final ODocument iDocument) {
  }

  @Override
  public void onRecordUpdateReplicated(final ODocument iDocument) {
  }

  @Override
  public RESULT onRecordBeforeDelete(final ODocument iDocument) {
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

    final OClass class_ = ODocumentInternal.getImmutableSchemaClass(iDocument);
    if (class_ != null) {
      final Collection<OIndex<?>> indexes = class_.getIndexes();

      final TreeMap<OIndex<?>, List<Object>> indexKeysMap = new TreeMap<OIndex<?>, List<Object>>();
      for (final OIndex<?> index : indexes) {
        if (index.getInternal() instanceof OIndexUnique) {
          OIndexRecorder indexRecorder = new OIndexRecorder((OIndexUnique) index.getInternal());

          addIndexEntry(iDocument, iDocument.getIdentity(), indexRecorder);
          indexKeysMap.put(index, indexRecorder.getAffectedKeys());
        }
      }

      if (noTx(iDocument)) {
        final List<Lock[]> locks = new ArrayList<Lock[]>(indexKeysMap.size());

        for (Map.Entry<OIndex<?>, List<Object>> entry : indexKeysMap.entrySet()) {
          final OIndexInternal<?> index = entry.getKey().getInternal();
          locks.add(index.lockKeysForUpdate(entry.getValue()));
        }

        lockedKeys.push(locks);
      }
    }

    return RESULT.RECORD_NOT_CHANGED;
  }

  @Override
  public void onRecordAfterDelete(final ODocument iDocument) {
    deleteIndexEntries(iDocument);
  }

  @Override
  public void onRecordDeleteFailed(final ODocument iDocument) {
  }

  @Override
  public void onRecordDeleteReplicated(final ODocument iDocument) {
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
      final Collection<OIndex<?>> indexes = cls.getIndexes();
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
    }

    return replaced;
  }

  @Override
  public void onRecordFinalizeUpdate(ODocument document) {
    if (noTx(document))
      unlockKeys();
  }

  @Override
  public void onRecordFinalizeCreation(ODocument document) {
    if (noTx(document))
      unlockKeys();
  }

  @Override
  public void onRecordFinalizeDeletion(ODocument document) {
    if (noTx(document))
      unlockKeys();
  }

  private void unlockKeys() {
    if (lockedKeys == null)
      return;

    final List<Lock[]> lockList = lockedKeys.poll();
    if (lockList == null)
      return;

    for (Lock[] locks : lockList) {
      for (Lock lock : locks)
        try {
          lock.unlock();
        } catch (RuntimeException e) {
          OLogManager.instance().error(this, "Error during unlock of index key", e);
        }
    }
  }

  protected void putInIndex(OIndex<?> index, Object key, OIdentifiable value) {
    index.put(key, value);
  }

  protected void removeFromIndex(OIndex<?> index, Object key, OIdentifiable value) {
    index.remove(key, value);
  }

  private static boolean noTx(ODocument document) {
    return !document.getDatabase().getTransaction().isActive();
  }
}
