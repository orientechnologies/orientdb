/*
 *
 *  *  Copyright 2010-2017 OrientDB LTD (http://orientdb.com)
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

package com.orientechnologies.orient.core.storage.impl.local;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.cache.OLocalRecordCache;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODirtyManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.storage.OBasicTransaction;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.tx.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The special micro transaction used to wrap non-transactional operations into implicit transactions. Such transactions are not
 * visible on the database level.
 *
 * @author Sergey Sitnikov
 */
public final class OMicroTransaction implements OBasicTransaction, OTransactionInternal {

  private static final AtomicInteger transactionSerial = new AtomicInteger(0);

  private final ODatabaseDocumentInternal database;
  private final OAbstractPaginatedStorage storage;

  private final int id;

  /**
   * All the record/cluster operations known to this micro-transactions, mapped by {@link ORID}.
   */
  private final Map<ORID, ORecordOperation> recordOperations = new LinkedHashMap<>();

  /**
   * All the index operations known to this micro-transaction, mapped by index name.
   */
  private final Map<String, OTransactionIndexChanges> indexOperations = new LinkedHashMap<>();

  /**
   * All the index operations known to this micro-transaction affecting index values containing records with certain {@link ORID}s,
   * mapped by the record {@link ORID}.
   */
  private final Map<ORID, List<OTransactionRecordIndexOperation>> recordIndexOperations = new HashMap<>();

  private final Map<ORID, ORID> updatedRids      = new HashMap<>();
  private final Set<ODocument>  changedDocuments = new HashSet<>();

  private final Map<String, Object> customData = new HashMap<>();

  private boolean active       = false;
  private int     level        = 0;
  private int     recordSerial = -2;

  /**
   * Instantiates a new micro-transaction.
   *
   * @param storage  the micro-transaction's storage.
   * @param database the micro-transaction's database.
   */
  public OMicroTransaction(OAbstractPaginatedStorage storage, ODatabaseDocumentInternal database) {
    this.storage = storage;
    this.database = database;

    this.id = transactionSerial.incrementAndGet();
  }

  /**
   * @return the unique id of this micro-transaction.
   */
  public int getId() {
    return id;
  }

  @Override
  public int getClientTransactionId() {
    return -1;
  }

  @Override
  public void updateIdentityAfterCommit(ORID oldRID, ORID rid) {
    updateIdentityAfterRecordCommit(oldRID, rid);
  }

  /**
   * @return the micro-transaction's database.
   */
  public ODatabaseDocumentInternal getDatabase() {
    return database;
  }

  /**
   * @return the record operations done in the context of this micro-transaction.
   */
  public Collection<ORecordOperation> getRecordOperations() { // ordered by operation time
    return recordOperations.values();
  }

  /**
   * @return the index operations done in the context of this micro-transaction.
   */
  public Map<String, OTransactionIndexChanges> getIndexOperations() {
    return indexOperations;
  }

  /**
   * Begins the micro-transaction. Micro-transactions may be nested.
   */
  public void begin() {
    if (level < 0)
      throw error("Unbalanced micro-transaction, level = " + level);

    ++level;
    active = true;
  }

  /**
   * Commits the micro-transaction if it's a top-level micro-transaction.
   */
  public void commit() {
    if (!active)
      throw error("Inactive micro-transaction on commit");
    if (level < 1)
      throw error("Unbalanced micro-transaction, level = " + level);

    --level;

    if (level == 0) {
      active = false;
      doCommit();
    }
  }

  /**
   * Rollbacks the micro-transaction if it's a top-level micro-transaction.
   */
  public void rollback() {
    if (!active)
      throw error("Inactive micro-transaction on rollback");
    if (level < 1)
      throw error("Unbalanced micro-transaction, level = " + level);

    --level;

    if (level == 0) {
      active = false;
      doRollback();
    }
  }

  /**
   * Rollbacks the micro-transaction after failed commit attempt.
   */
  public void rollbackAfterFailedCommit() {
    if (active)
      throw error("Active micro-transaction on rollback after failed commit");
    if (level != 0)
      throw error("Unbalanced micro-transaction, level = " + level);

    doRollback();
  }

  /**
   * Updates the record identity after its successful commit.
   */
  public void updateIdentityAfterRecordCommit(final ORID oldRid, final ORID newRid) {
    if (oldRid.equals(newRid))
      return; // no change, ignore

    // XXX: Identity update may mutate the index keys, so we have to identify and reinsert potentially affected index keys to keep
    // the OTransactionIndexChanges.changesPerKey in a consistent state.

    final List<KeyChangesUpdateRecord> keyRecordsToReinsert = new ArrayList<>();
    final OIndexManager indexManager = getDatabase().getMetadata().getIndexManager();
    for (Map.Entry<String, OTransactionIndexChanges> entry : indexOperations.entrySet()) {
      final OIndex<?> index = indexManager.getIndex(entry.getKey());
      if (index == null)
        throw new OTransactionException("Cannot find index '" + entry.getValue() + "' while committing transaction");

      final Dependency[] fieldRidDependencies = getIndexFieldRidDependencies(index);
      if (!isIndexMayDependOnRids(fieldRidDependencies))
        continue;

      final OTransactionIndexChanges indexChanges = entry.getValue();
      for (final Iterator<OTransactionIndexChangesPerKey> iterator = indexChanges.changesPerKey.values().iterator(); iterator
          .hasNext(); ) {
        final OTransactionIndexChangesPerKey keyChanges = iterator.next();
        if (isIndexKeyMayDependOnRid(keyChanges.key, oldRid, fieldRidDependencies)) {
          keyRecordsToReinsert.add(new KeyChangesUpdateRecord(keyChanges, indexChanges));
          iterator.remove();
        }
      }
    }

    // Update the identity.

    final ORecordOperation rec = resolveRecordOperation(oldRid);
    if (rec != null) {
      updatedRids.put(newRid.copy(), oldRid.copy());

      if (!rec.getRecord().getIdentity().equals(newRid)) {
        ORecordInternal.onBeforeIdentityChanged(rec.getRecord());

        final ORecordId recordId = (ORecordId) rec.getRecord().getIdentity();
        if (recordId == null) {
          ORecordInternal.setIdentity(rec.getRecord(), new ORecordId(newRid));
        } else {
          recordId.setClusterPosition(newRid.getClusterPosition());
          recordId.setClusterId(newRid.getClusterId());
        }

        ORecordInternal.onAfterIdentityChanged(rec.getRecord());
      }
    }

    // Reinsert the potentially affected index keys.

    for (KeyChangesUpdateRecord record : keyRecordsToReinsert)
      record.indexChanges.changesPerKey.put(record.keyChanges.key, record.keyChanges);

    // Update the indexes.

    final List<OTransactionRecordIndexOperation> transactionIndexOperations = recordIndexOperations.get(translateRid(oldRid));
    if (transactionIndexOperations != null) {
      for (final OTransactionRecordIndexOperation indexOperation : transactionIndexOperations) {
        OTransactionIndexChanges indexEntryChanges = indexOperations.get(indexOperation.index);
        if (indexEntryChanges == null)
          continue;
        final OTransactionIndexChangesPerKey keyChanges;
        if (indexOperation.key == null) {
          keyChanges = indexEntryChanges.nullKeyChanges;
        } else {
          keyChanges = indexEntryChanges.changesPerKey.get(indexOperation.key);
        }
        if (keyChanges != null)
          updateChangesIdentity(oldRid, newRid, keyChanges);
      }
    }
  }

  /**
   * Updates the record cache after unsuccessful micro-transaction commit.
   */
  public void updateRecordCacheAfterRollback() {
    final OLocalRecordCache databaseLocalCache = database.getLocalCache();

    for (ORecordOperation recordOperation : recordOperations.values())
      databaseLocalCache.deleteRecord(recordOperation.getRecord().getIdentity());
  }

  @Override
  public boolean isActive() {
    return active;
  }

  @Override
  public ORecord saveRecord(ORecord record, String clusterName, ODatabase.OPERATION_MODE operationMode, boolean forceCreation,
      ORecordCallback<? extends Number> createdCallback, ORecordCallback<Integer> updatedCallback) {
    if (!active)
      throw error("Inactive micro-transaction on record save");

    if (record == null)
      return null;
    if (!record.isDirty())
      return record;

    final ORecordOperation recordOperation;
    if (forceCreation || !record.getIdentity().isValid())
      recordOperation = addRecordOperation(record, ORecordOperation.CREATED, clusterName);
    else
      recordOperation = addRecordOperation(record, ORecordOperation.UPDATED, clusterName);

    if (recordOperation != null) {
      if (createdCallback != null)
        //noinspection unchecked
        recordOperation.createdCallback = (ORecordCallback<Long>) createdCallback;
      if (updatedCallback != null)
        recordOperation.updatedCallback = updatedCallback;
    }

    return record;
  }

  @Override
  public void deleteRecord(ORecord record, ODatabase.OPERATION_MODE mode) {
    if (!record.getIdentity().isValid())
      return;

    addRecordOperation(record, ORecordOperation.DELETED, null);
  }

  @Override
  public ORecord getRecord(ORID rid) {
    final ORecordOperation recordOperation = resolveRecordOperation(rid);
    if (recordOperation == null)
      return null;

    return recordOperation.type == ORecordOperation.DELETED ? DELETED_RECORD : recordOperation.record.getRecord();
  }

  @Override
  public OTransactionIndexChanges getIndexChanges(String indexName) {
    return indexOperations.get(indexName);
  }

  @Override
  public OTransactionIndexChanges getIndexChangesInternal(String indexName) {
    return getIndexChanges(indexName);
  }

  @Override
  public Object getCustomData(String name) {
    return customData.get(name);
  }

  @Override
  public void setCustomData(String name, Object value) {
    customData.put(name, value);
  }

  private OStorageException error(String message) {
    return new OStorageException(message);
  }

  private void doCommit() {
    if (!recordOperations.isEmpty() || !indexOperations.isEmpty())
      getDatabase().internalCommit(this);

    invokeCallbacks();

    reset();
  }

  private void doRollback() {
    storage.rollback(this);

    database.getLocalCache().clear();

    for (ORecordOperation recordOperation : recordOperations.values()) {
      final ORecord record = recordOperation.record.getRecord();

      if (record.isDirty())
        if (record instanceof ODocument) {
          final ODocument document = (ODocument) record;

          if (document.isTrackingChanges())
            document.undo();
        } else
          record.unload();
    }

    reset();
  }

  private void invokeCallbacks() {
    for (ORecordOperation recordOperation : recordOperations.values()) {
      final ORecord record = recordOperation.getRecord();
      final ORID identity = record.getIdentity();

      if (recordOperation.type == ORecordOperation.CREATED && recordOperation.createdCallback != null)
        recordOperation.createdCallback.call(new ORecordId(identity), identity.getClusterPosition());
      else if (recordOperation.type == ORecordOperation.UPDATED && recordOperation.updatedCallback != null)
        recordOperation.updatedCallback.call(new ORecordId(identity), record.getVersion());
    }
  }

  private void reset() {
    for (ORecordOperation recordOperation : recordOperations.values()) {
      final ORecord record = recordOperation.record.getRecord();

      if (record instanceof ODocument) {
        final ODocument document = (ODocument) record;

        if (document.isDirty())
          document.undo();

        changedDocuments.remove(document);
      }
    }

    for (ODocument changedDocument : changedDocuments)
      changedDocument.undo();

    changedDocuments.clear();
    updatedRids.clear();
    recordOperations.clear();
    indexOperations.clear();
    recordIndexOperations.clear();
    recordSerial = -2;

    customData.clear();
  }

  private ORecordOperation resolveRecordOperation(ORID rid) {
    return recordOperations.get(translateRid(rid));
  }

  private ORID translateRid(ORID rid) {
    while (true) {
      final ORID translatedRid = updatedRids.get(rid);
      if (translatedRid == null)
        break;

      rid = translatedRid;
    }

    return rid;
  }

  private ORecordOperation addRecordOperation(ORecord record, byte type, String clusterName) {

    if (clusterName == null)
      clusterName = database.getClusterNameById(record.getIdentity().getClusterId());

    //noinspection SuspiciousMethodCalls
    changedDocuments.remove(record);

    try {
      switch (type) {
      case ORecordOperation.CREATED: {
        database.checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_CREATE, clusterName);
        ORecordHook.RESULT result = database.callbackHooks(ORecordHook.TYPE.BEFORE_CREATE, record);
        if (result == ORecordHook.RESULT.RECORD_CHANGED && record instanceof ODocument)
          ((ODocument) record).validate();
        if (result == ORecordHook.RESULT.RECORD_CHANGED)
          reSave(record);
      }
      break;

      case ORecordOperation.UPDATED: {
        database.checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_UPDATE, clusterName);
        ORecordHook.RESULT result = database.callbackHooks(ORecordHook.TYPE.BEFORE_UPDATE, record);
        if (result == ORecordHook.RESULT.RECORD_CHANGED && record instanceof ODocument)
          ((ODocument) record).validate();
        if (result == ORecordHook.RESULT.RECORD_CHANGED)
          reSave(record);
      }
      break;

      case ORecordOperation.DELETED:
        database.checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_DELETE, clusterName);
        database.callbackHooks(ORecordHook.TYPE.BEFORE_DELETE, record);
        break;
      }

      try {
        final ORecordId recordId = (ORecordId) record.getIdentity();

        if (!recordId.isValid()) {
          ORecordInternal.onBeforeIdentityChanged(record);
          database.assignAndCheckCluster(record, clusterName);

          recordId.setClusterPosition(recordSerial--);

          ORecordInternal.onAfterIdentityChanged(record);
        }

        ORecordOperation recordOperation = resolveRecordOperation(recordId);

        if (recordOperation == null) {
          if (!(recordId.isTemporary() && type != ORecordOperation.CREATED)) {
            recordOperation = new ORecordOperation(record, type);
            recordOperations.put(recordId.copy(), recordOperation);
          }
        } else {
          recordOperation.record = record;

          switch (recordOperation.type) {
          case ORecordOperation.CREATED:
            if (type == ORecordOperation.DELETED)
              recordOperations.remove(recordId);
            break;
          case ORecordOperation.UPDATED:
            if (type == ORecordOperation.DELETED)
              recordOperation.type = ORecordOperation.DELETED;
            break;
          case ORecordOperation.DELETED:
            break; // do nothing
          }
        }

        switch (type) {
        case ORecordOperation.CREATED:
          database.callbackHooks(ORecordHook.TYPE.AFTER_CREATE, record);
          break;
        case ORecordOperation.UPDATED:
          database.callbackHooks(ORecordHook.TYPE.AFTER_UPDATE, record);
          break;
        case ORecordOperation.DELETED:
          database.callbackHooks(ORecordHook.TYPE.AFTER_DELETE, record);
          break;
        }

        if (record instanceof ODocument && ((ODocument) record).isTrackingChanges())
          ODocumentInternal.clearTrackData(((ODocument) record));

        return recordOperation;

      } catch (Exception e) {
        switch (type) {
        case ORecordOperation.CREATED:
          database.callbackHooks(ORecordHook.TYPE.CREATE_FAILED, record);
          break;
        case ORecordOperation.UPDATED:
          database.callbackHooks(ORecordHook.TYPE.UPDATE_FAILED, record);
          break;
        case ORecordOperation.DELETED:
          database.callbackHooks(ORecordHook.TYPE.DELETE_FAILED, record);
          break;
        }

        throw OException.wrapException(new ODatabaseException("Error on saving record " + record.getIdentity()), e);
      }
    } finally {
      switch (type) {
      case ORecordOperation.CREATED:
        database.callbackHooks(ORecordHook.TYPE.FINALIZE_CREATION, record);
        break;
      case ORecordOperation.UPDATED:
        database.callbackHooks(ORecordHook.TYPE.FINALIZE_UPDATE, record);
        break;
      case ORecordOperation.DELETED:
        database.callbackHooks(ORecordHook.TYPE.FINALIZE_DELETION, record);
        break;
      }
    }
  }

  @Override
  public void addIndexEntry(OIndex<?> index, String indexName, OTransactionIndexChanges.OPERATION type, Object key,
      OIdentifiable value) {
    final OTransactionIndexChanges indexOperation = indexOperations.computeIfAbsent(indexName, k -> new OTransactionIndexChanges());

    if (type == OTransactionIndexChanges.OPERATION.CLEAR)
      indexOperation.setCleared();
    else {
      final OTransactionIndexChangesPerKey changesPerKey = indexOperation.getChangesPerKey(key);
      changesPerKey.clientTrackOnly = false;
      changesPerKey.add(value, type);

      if (value == null)
        return;

      List<OTransactionRecordIndexOperation> transactionIndexOperations = recordIndexOperations.get(value.getIdentity());
      if (transactionIndexOperations == null) {
        transactionIndexOperations = new ArrayList<>();
        recordIndexOperations.put(value.getIdentity().copy(), transactionIndexOperations);
      }

      transactionIndexOperations.add(new OTransactionRecordIndexOperation(indexName, key, type));
    }
  }

  @Override
  public void addChangedDocument(ODocument document) {
    if (getRecord(document.getIdentity()) == null)
      changedDocuments.add(document);
  }

  private void reSave(ORecord record) {
    final ODirtyManager manager = ORecordInternal.getDirtyManager(record);
    final Set<ORecord> newRecords = manager.getNewRecords();
    final Set<ORecord> updatedRecords = manager.getUpdateRecords();
    manager.clearForSave();

    if (newRecords != null) {
      for (ORecord newRecord : newRecords) {
        if (newRecord != record)
          saveRecord(newRecord, null, ODatabase.OPERATION_MODE.SYNCHRONOUS, false, null, null);
      }
    }

    if (updatedRecords != null) {
      for (ORecord updatedRecord : updatedRecords) {
        if (updatedRecord != record)
          saveRecord(updatedRecord, null, ODatabase.OPERATION_MODE.SYNCHRONOUS, false, null, null);
      }
    }
  }

  private void updateChangesIdentity(ORID oldRid, ORID newRid, OTransactionIndexChangesPerKey changesPerKey) {
    if (changesPerKey == null)
      return;

    for (final OTransactionIndexChangesPerKey.OTransactionIndexEntry indexEntry : changesPerKey.entries)
      if (indexEntry.value.getIdentity().equals(oldRid))
        indexEntry.value = newRid;
  }

  private static Dependency[] getIndexFieldRidDependencies(OIndex<?> index) {
    final OIndexDefinition definition = index.getDefinition();

    if (definition == null) // type for untyped index is still not resolved
      return null;

    final OType[] types = definition.getTypes();
    final Dependency[] dependencies = new Dependency[types.length];

    for (int i = 0; i < types.length; ++i)
      dependencies[i] = getTypeRidDependency(types[i]);

    return dependencies;
  }

  private static Dependency getTypeRidDependency(OType type) {
    switch (type) {
    case CUSTOM:
    case ANY:
      return Dependency.Unknown;

    case EMBEDDED:
    case LINK:
      return Dependency.Yes;

    case LINKLIST:
    case LINKSET:
    case LINKMAP:
    case LINKBAG:
    case EMBEDDEDLIST:
    case EMBEDDEDSET:
    case EMBEDDEDMAP:
      assert false; // under normal conditions, collection field type is already resolved to its component type
      return Dependency.Unknown; // fallback to the safest variant, just in case

    default: // all other primitive types which doesn't depend on rids
      return Dependency.No;
    }
  }

  private static boolean isIndexMayDependOnRids(Dependency[] fieldDependencies) {
    if (fieldDependencies == null)
      return true;

    for (Dependency dependency : fieldDependencies)
      switch (dependency) {
      case Unknown:
        return true;
      case Yes:
        return true;
      case No:
        break; // do nothing
      }

    return false;
  }

  private static boolean isIndexKeyMayDependOnRid(Object key, ORID rid, Dependency[] keyDependencies) {
    if (key instanceof OCompositeKey) {
      final List<Object> subKeys = ((OCompositeKey) key).getKeys();
      for (int i = 0; i < subKeys.size(); ++i)
        if (isIndexKeyMayDependOnRid(subKeys.get(i), rid, keyDependencies == null ? null : keyDependencies[i]))
          return true;
      return false;
    }

    return isIndexKeyMayDependOnRid(key, rid, keyDependencies == null ? null : keyDependencies[0]);
  }

  private static boolean isIndexKeyMayDependOnRid(Object key, ORID rid, Dependency dependency) {
    if (dependency == Dependency.No)
      return false;

    if (key instanceof OIdentifiable)
      return key.equals(rid);

    return dependency == Dependency.Unknown || dependency == null;
  }

  @Override
  public void setStatus(OTransaction.TXSTATUS iStatus) {
    //IGNORE
  }

  private enum Dependency {
    Unknown, Yes, No
  }

  private static class KeyChangesUpdateRecord {
    final OTransactionIndexChangesPerKey keyChanges;
    final OTransactionIndexChanges       indexChanges;

    public KeyChangesUpdateRecord(OTransactionIndexChangesPerKey keyChanges, OTransactionIndexChanges indexChanges) {
      this.keyChanges = keyChanges;
      this.indexChanges = indexChanges;
    }
  }

  @Override
  public boolean isUsingLog() {
    return true;
  }

  @Override
  public ORecordOperation getRecordEntry(ORID currentRid) {
    return recordOperations.get(currentRid);
  }
}
