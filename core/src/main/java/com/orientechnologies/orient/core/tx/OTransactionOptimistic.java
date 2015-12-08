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

package com.orientechnologies.orient.core.tx;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabase.OPERATION_MODE;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal.RUN_MODE;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.engine.local.OEngineLocalPaginated;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.hook.ORecordHook.RESULT;
import com.orientechnologies.orient.core.hook.ORecordHook.TYPE;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexAbstract;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.version.ORecordVersion;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public class OTransactionOptimistic extends OTransactionRealAbstract {
  private static AtomicInteger txSerial = new AtomicInteger();

  private boolean usingLog = true;
  private int     txStartCounter;

  private class CommitIndexesCallback implements Runnable {
    private final Map<String, OIndex<?>> indexes;

    private CommitIndexesCallback(Map<String, OIndex<?>> indexes) {
      this.indexes = indexes;
    }

    @Override
    public void run() {
      final ODocument indexEntries = getIndexChanges();
      if (indexEntries != null) {
        final Map<String, OIndexInternal<?>> indexesToCommit = new HashMap<String, OIndexInternal<?>>();

        for (Entry<String, Object> indexEntry : indexEntries) {
          final OIndexInternal<?> index = indexes.get(indexEntry.getKey()).getInternal();
          indexesToCommit.put(index.getName(), index.getInternal());
        }

        for (OIndexInternal<?> indexInternal : indexesToCommit.values())
          indexInternal.preCommit();

        for (Entry<String, Object> indexEntry : indexEntries) {
          final OIndexInternal<?> index = indexesToCommit.get(indexEntry.getKey()).getInternal();

          if (index == null) {
            OLogManager.instance().error(this, "Index with name '" + indexEntry.getKey() + "' was not found.");
            throw new OIndexException("Index with name '" + indexEntry.getKey() + "' was not found.");
          } else
            index.addTxOperation((ODocument) indexEntry.getValue());
        }

        try {
          for (OIndexInternal<?> indexInternal : indexesToCommit.values())
            indexInternal.commit();
        } finally {
          for (OIndexInternal<?> indexInternal : indexesToCommit.values())
            indexInternal.postCommit();
        }
      }
    }
  }

  public OTransactionOptimistic(final ODatabaseDocumentTx iDatabase) {
    super(iDatabase, txSerial.incrementAndGet());
  }

  public void begin() {
    if (txStartCounter < 0)
      throw new OTransactionException("Invalid value of TX counter.");

    if (txStartCounter == 0)
      status = TXSTATUS.BEGUN;

    txStartCounter++;

    if (txStartCounter > 1)
      OLogManager.instance().debug(this, "Transaction was already started and will be reused.");
  }

  public void commit() {
    commit(false);
  }

  /**
   * The transaction is reentrant. If {@code begin()} has been called several times, the actual commit happens only after the same
   * amount of {@code commit()} calls
   *
   * @param force
   *          commit transaction even
   */
  @Override
  public void commit(final boolean force) {
    checkTransaction();

    if (txStartCounter < 0)
      throw new OStorageException("Invalid value of tx counter");

    if (force)
      txStartCounter = 0;
    else
      txStartCounter--;

    if (txStartCounter == 0) {
      doCommit();
    } else if (txStartCounter > 0)
      OLogManager.instance().debug(this, "Nested transaction was closed but transaction itself was not committed.");
    else
      throw new OTransactionException("Transaction was committed more times than it is started.");
  }

  @Override
  public int amountOfNestedTxs() {
    return txStartCounter;
  }

  public void rollback() {
    rollback(false, -1);
  }

  @Override
  public void rollback(boolean force, int commitLevelDiff) {
    if (txStartCounter < 0)
      throw new OStorageException("Invalid value of TX counter");

    checkTransaction();

    txStartCounter += commitLevelDiff;
    status = TXSTATUS.ROLLBACKING;

    if (!force && txStartCounter > 0) {
      OLogManager.instance().debug(this, "Nested transaction was closed but transaction itself was scheduled for rollback.");
      return;
    }

    if (txStartCounter < 0)
      throw new OTransactionException("Transaction was rolled back more times than it was started.");

    database.getStorage().callInLock(new Callable<Void>() {

      public Void call() throws Exception {

        database.getStorage().rollback(OTransactionOptimistic.this);
        return null;
      }
    }, true);

    // CLEAR THE CACHE
    database.getLocalCache().clear();

    // REMOVE ALL THE ENTRIES AND INVALIDATE THE DOCUMENTS TO AVOID TO BE RE-USED DIRTY AT USER-LEVEL. IN THIS WAY RE-LOADING MUST
    // EXECUTED
    for (ORecordOperation v : recordEntries.values())
      v.getRecord().unload();

    for (ORecordOperation v : allEntries.values())
      v.getRecord().unload();

    close();

    status = TXSTATUS.ROLLED_BACK;
  }

  public ORecord loadRecord(final ORID rid, final ORecord iRecord, final String fetchPlan, final boolean ignoreCache,
      final boolean loadTombstone, final OStorage.LOCKING_STRATEGY lockingStrategy) {
    return loadRecord(rid, iRecord, fetchPlan, ignoreCache, true, loadTombstone, lockingStrategy);
  }

  public ORecord loadRecord(final ORID rid, final ORecord iRecord, final String fetchPlan, final boolean ignoreCache,
      final boolean iUpdateCache, final boolean loadTombstone, final OStorage.LOCKING_STRATEGY lockingStrategy) {
    checkTransaction();

    final ORecord txRecord = getRecord(rid);
    if (txRecord == OTransactionRealAbstract.DELETED_RECORD)
      // DELETED IN TX
      return null;

    if (txRecord != null) {
      if (iRecord != null && txRecord != iRecord)
        OLogManager.instance().warn(this,
            "Found record in transaction with the same RID %s but different instance. "
                + "Probably the record has been loaded from another transaction and reused on the current one: reload it "
                + "from current transaction before to update or delete it",
            iRecord.getIdentity());
      return txRecord;
    }

    if (rid.isTemporary())
      return null;

    // DELEGATE TO THE STORAGE, NO TOMBSTONES SUPPORT IN TX MODE
    final ORecord record = database.executeReadRecord((ORecordId) rid, iRecord, null, fetchPlan, ignoreCache, iUpdateCache, false,
        lockingStrategy, new ODatabaseDocumentTx.SimpleRecordReader());

    if (record != null && isolationLevel == ISOLATION_LEVEL.REPEATABLE_READ)
      // KEEP THE RECORD IN TX TO ASSURE REPEATABLE READS
      addRecord(record, ORecordOperation.LOADED, null);

    return record;
  }

  @Override
  public ORecord loadRecordIfVersionIsNotLatest(ORID rid, ORecordVersion recordVersion, String fetchPlan, boolean ignoreCache)
      throws ORecordNotFoundException {
    checkTransaction();

    final ORecord txRecord = getRecord(rid);
    if (txRecord == OTransactionRealAbstract.DELETED_RECORD)
      // DELETED IN TX
      throw new ORecordNotFoundException("Record with id " + rid + " was not found in database.");

    if (txRecord != null) {
      if (txRecord.getRecordVersion().compareTo(recordVersion) > 0)
        return txRecord;
      else
        return null;
    }

    if (rid.isTemporary())
      throw new ORecordNotFoundException("Record with id " + rid + " was not found in database.");

    // DELEGATE TO THE STORAGE, NO TOMBSTONES SUPPORT IN TX MODE
    final ORecord record = database.executeReadRecord((ORecordId) rid, null, recordVersion, fetchPlan, ignoreCache, !ignoreCache,
        false, OStorage.LOCKING_STRATEGY.NONE, new ODatabaseDocumentTx.SimpleRecordReader());

    if (record != null && isolationLevel == ISOLATION_LEVEL.REPEATABLE_READ)
      // KEEP THE RECORD IN TX TO ASSURE REPEATABLE READS
      addRecord(record, ORecordOperation.LOADED, null);

    return record;
  }

  @Override
  public ORecord reloadRecord(ORID rid, ORecord iRecord, String fetchPlan, boolean ignoreCache) {
    return reloadRecord(rid, iRecord, fetchPlan, ignoreCache, true);
  }

  @Override
  public ORecord reloadRecord(ORID rid, ORecord passedRecord, String fetchPlan, boolean ignoreCache, boolean force) {
    checkTransaction();

    final ORecord txRecord = getRecord(rid);
    if (txRecord == OTransactionRealAbstract.DELETED_RECORD)
      // DELETED IN TX
      return null;

    if (txRecord != null) {
      if (passedRecord != null && txRecord != passedRecord)
        OLogManager.instance().warn(this,
            "Found record in transaction with the same RID %s but different instance. "
                + "Probably the record has been loaded from another transaction and reused on the current one: reload it "
                + "from current transaction before to update or delete it",
            passedRecord.getIdentity());
      return txRecord;
    }

    if (rid.isTemporary())
      return null;

    // DELEGATE TO THE STORAGE, NO TOMBSTONES SUPPORT IN TX MODE
    final ORecord record;
    try {
      final ODatabaseDocumentTx.RecordReader recordReader;
      if (force) {
        recordReader = new ODatabaseDocumentTx.SimpleRecordReader();
      } else {
        recordReader = new ODatabaseDocumentTx.LatestVersionRecordReader();
      }

      ORecord loadedRecord = database.executeReadRecord((ORecordId) rid, passedRecord, null, fetchPlan, ignoreCache, !ignoreCache,
          false, OStorage.LOCKING_STRATEGY.NONE, recordReader);

      if (force) {
        record = loadedRecord;
      } else {
        if (loadedRecord == null)
          record = passedRecord;
        else
          record = loadedRecord;
      }

    } catch (ORecordNotFoundException e) {
      return null;
    }

    if (record != null && isolationLevel == ISOLATION_LEVEL.REPEATABLE_READ)
      // KEEP THE RECORD IN TX TO ASSURE REPEATABLE READS
      addRecord(record, ORecordOperation.LOADED, null);

    return record;

  }

  @Override
  public ORecord loadRecord(ORID rid, ORecord record, String fetchPlan, boolean ignoreCache) {
    return loadRecord(rid, record, fetchPlan, ignoreCache, false, OStorage.LOCKING_STRATEGY.NONE);
  }

  public void deleteRecord(final ORecord iRecord, final OPERATION_MODE iMode) {
    if (!iRecord.getIdentity().isValid())
      return;

    addRecord(iRecord, ORecordOperation.DELETED, null);
  }

  public ORecord saveRecord(final ORecord iRecord, final String iClusterName, final OPERATION_MODE iMode, boolean iForceCreate,
      final ORecordCallback<? extends Number> iRecordCreatedCallback, ORecordCallback<ORecordVersion> iRecordUpdatedCallback) {
    if (iRecord == null)
      return null;
    final byte operation = iForceCreate ? ORecordOperation.CREATED
        : iRecord.getIdentity().isValid() ? ORecordOperation.UPDATED : ORecordOperation.CREATED;
    addRecord(iRecord, operation, iClusterName);
    return iRecord;
  }

  @Override
  public String toString() {
    return "OTransactionOptimistic [id=" + id + ", status=" + status + ", recEntries=" + recordEntries.size() + ", idxEntries="
        + indexEntries.size() + ']';
  }

  public boolean isUsingLog() {
    return usingLog;
  }

  public void setUsingLog(final boolean useLog) {
    this.usingLog = useLog;
  }

  public void setStatus(final TXSTATUS iStatus) {
    status = iStatus;
  }

  public void addRecord(final ORecord iRecord, final byte iStatus, final String iClusterName) {
    checkTransaction();

    if (iStatus != ORecordOperation.LOADED)
      changedDocuments.remove(iRecord);

    try {
      switch (iStatus) {
      case ORecordOperation.CREATED: {
        database.checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_CREATE, iClusterName);
        RESULT res = database.callbackHooks(TYPE.BEFORE_CREATE, iRecord);
        if (res == RESULT.RECORD_CHANGED && iRecord instanceof ODocument)
          ((ODocument) iRecord).validate();
      }
        break;
      case ORecordOperation.LOADED:
        /**
         * Read hooks already invoked in {@link com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx#executeReadRecord}
         * .
         */
        break;
      case ORecordOperation.UPDATED: {
        database.checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_UPDATE, iClusterName);
        RESULT res = database.callbackHooks(TYPE.BEFORE_UPDATE, iRecord);
        if (res == RESULT.RECORD_CHANGED && iRecord instanceof ODocument)
          ((ODocument) iRecord).validate();
      }
        break;

      case ORecordOperation.DELETED:
        database.checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_DELETE, iClusterName);
        database.callbackHooks(TYPE.BEFORE_DELETE, iRecord);
        break;
      }

      try {
        if (iRecord.getIdentity().isTemporary())
          temp2persistent.put(iRecord.getIdentity().copy(), iRecord);

        if ((status == OTransaction.TXSTATUS.COMMITTING)
            && database.getStorage().getUnderlying() instanceof OAbstractPaginatedStorage) {

          // I'M COMMITTING: BYPASS LOCAL BUFFER
          switch (iStatus) {
          case ORecordOperation.CREATED:
          case ORecordOperation.UPDATED:
            final ORID oldRid = iRecord.getIdentity().copy();
            database.executeSaveRecord(iRecord, iClusterName, iRecord.getRecordVersion(), false, OPERATION_MODE.SYNCHRONOUS, false,
                null, null);
            updateIdentityAfterCommit(oldRid, iRecord.getIdentity());
            break;
          case ORecordOperation.DELETED:
            database.executeDeleteRecord(iRecord, iRecord.getRecordVersion(), false, false, OPERATION_MODE.SYNCHRONOUS, false);
            break;
          }

          final ORecordOperation txRecord = getRecordEntry(iRecord.getIdentity());

          if (txRecord == null) {
            // NOT IN TX, SAVE IT ANYWAY
            allEntries.put(iRecord.getIdentity(), new ORecordOperation(iRecord, iStatus));
          } else if (txRecord.record != iRecord) {
            // UPDATE LOCAL RECORDS TO AVOID MISMATCH OF VERSION/CONTENT
            final String clusterName = getDatabase().getClusterNameById(iRecord.getIdentity().getClusterId());
            if (!clusterName.equals(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME)
                && !clusterName.equals(OMetadataDefault.CLUSTER_INDEX_NAME))
              OLogManager.instance().warn(this,
                  "Found record in transaction with the same RID %s but different instance. Probably the record has been loaded from another transaction and reused on the current one: reload it from current transaction before to update or delete it",
                  iRecord.getIdentity());

            txRecord.record = iRecord;
            txRecord.type = iStatus;
          }

        } else {
          final ORecordId rid = (ORecordId) iRecord.getIdentity();

          if (!rid.isValid()) {
            ORecordInternal.onBeforeIdentityChanged(iRecord);
            if (database.getStorage().isAssigningClusterIds() || iClusterName != null) {
              // ASSIGN A UNIQUE SERIAL TEMPORARY ID
              if (rid.clusterId == ORID.CLUSTER_ID_INVALID)
                rid.clusterId = iClusterName != null ? database.getClusterIdByName(iClusterName) : database.getDefaultClusterId();

              if (database.getStorageVersions().classesAreDetectedByClusterId() && iRecord instanceof ODocument) {
                final ODocument recordSchemaAware = (ODocument) iRecord;
                final OClass recordClass = ODocumentInternal.getImmutableSchemaClass(recordSchemaAware);
                final OClass clusterIdClass = ((OMetadataInternal) database.getMetadata()).getImmutableSchemaSnapshot()
                    .getClassByClusterId(rid.clusterId);
                if (recordClass == null && clusterIdClass != null || clusterIdClass == null && recordClass != null
                    || (recordClass != null && !recordClass.equals(clusterIdClass)))
                  throw new OSchemaException("Record saved into cluster " + iClusterName + " should be saved with class "
                      + clusterIdClass + " but saved with class " + recordClass);
              }
            }

            rid.clusterPosition = newObjectCounter--;

            ORecordInternal.onAfterIdentityChanged(iRecord);
          }

          ORecordOperation txEntry = getRecordEntry(rid);

          if (txEntry == null) {
            if (!(rid.isTemporary() && iStatus != ORecordOperation.CREATED)) {
              // NEW ENTRY: JUST REGISTER IT
              txEntry = new ORecordOperation(iRecord, iStatus);
              recordEntries.put(rid, txEntry);
            }
          } else {
            // UPDATE PREVIOUS STATUS
            txEntry.record = iRecord;

            switch (txEntry.type) {
            case ORecordOperation.LOADED:
              switch (iStatus) {
              case ORecordOperation.UPDATED:
                txEntry.type = ORecordOperation.UPDATED;
                break;
              case ORecordOperation.DELETED:
                txEntry.type = ORecordOperation.DELETED;
                break;
              }
              break;
            case ORecordOperation.UPDATED:
              switch (iStatus) {
              case ORecordOperation.DELETED:
                txEntry.type = ORecordOperation.DELETED;
                break;
              }
              break;
            case ORecordOperation.DELETED:
              break;
            case ORecordOperation.CREATED:
              switch (iStatus) {
              case ORecordOperation.DELETED:
                recordEntries.remove(rid);
                // txEntry.type = ORecordOperation.DELETED;
                break;
              }
              break;
            }
          }
        }

        switch (iStatus) {
        case ORecordOperation.CREATED:
          database.callbackHooks(TYPE.AFTER_CREATE, iRecord);
          break;
        case ORecordOperation.LOADED:
          /**
           * Read hooks already invoked in
           * {@link com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx#executeReadRecord} .
           */
          break;
        case ORecordOperation.UPDATED:
          database.callbackHooks(TYPE.AFTER_UPDATE, iRecord);
          break;
        case ORecordOperation.DELETED:
          database.callbackHooks(TYPE.AFTER_DELETE, iRecord);
          break;
        }

        // RESET TRACKING
        if (iRecord instanceof ODocument && ((ODocument) iRecord).isTrackingChanges()) {
          ((ODocument) iRecord).setTrackingChanges(false);
          ((ODocument) iRecord).setTrackingChanges(true);
        }

      } catch (Throwable t) {
        switch (iStatus) {
        case ORecordOperation.CREATED:
          database.callbackHooks(TYPE.CREATE_FAILED, iRecord);
          break;
        case ORecordOperation.UPDATED:
          database.callbackHooks(TYPE.UPDATE_FAILED, iRecord);
          break;
        case ORecordOperation.DELETED:
          database.callbackHooks(TYPE.DELETE_FAILED, iRecord);
          break;
        }

        if (t instanceof RuntimeException)
          throw (RuntimeException) t;
        else
          throw new ODatabaseException("Error on saving record " + iRecord.getIdentity(), t);
      }
    } finally {
      switch (iStatus) {
      case ORecordOperation.CREATED:
        database.callbackHooks(TYPE.FINALIZE_CREATION, iRecord);
        break;
      case ORecordOperation.UPDATED:
        database.callbackHooks(TYPE.FINALIZE_UPDATE, iRecord);
        break;
      }
    }
  }

  private void doCommit() {
    if (status == TXSTATUS.ROLLED_BACK || status == TXSTATUS.ROLLBACKING)
      throw new ORollbackException("Given transaction was rolled back and cannot be used.");

    status = TXSTATUS.COMMITTING;

    if (!recordEntries.isEmpty() || !indexEntries.isEmpty()) {
      if (OScenarioThreadLocal.INSTANCE.getRunMode() == RUN_MODE.DEFAULT
          && !(database.getStorage().getUnderlying() instanceof OAbstractPaginatedStorage))
        database.getStorage().commit(this, null);
      else {
        List<OIndexAbstract<?>> lockedIndexes = acquireIndexLocks();
        try {
          final Map<String, OIndex<?>> indexes = new HashMap<String, OIndex<?>>();
          for (OIndex<?> index : database.getMetadata().getIndexManager().getIndexes())
            indexes.put(index.getName(), index);

          final Runnable callback = new CommitIndexesCallback(indexes);

          final String storageType = database.getStorage().getUnderlying().getType();

          if (storageType.equals(OEngineLocalPaginated.NAME) || storageType.equals(OEngineMemory.NAME))
            database.getStorage().commit(OTransactionOptimistic.this, callback);
          else {
            database.getStorage().callInLock(new Callable<Object>() {
              @Override
              public Object call() throws Exception {
                database.getStorage().commit(OTransactionOptimistic.this, callback);
                return null;
              }
            }, true);
          }
        } finally {
          releaseIndexLocks(lockedIndexes);
        }
      }
    }

    close();

    status = TXSTATUS.COMPLETED;
  }

  private List<OIndexAbstract<?>> acquireIndexLocks() {
    List<OIndexAbstract<?>> lockedIndexes = null;
    final List<String> involvedIndexes = getInvolvedIndexes();

    if (involvedIndexes != null)
      Collections.sort(involvedIndexes);

    try {
      // LOCK INVOLVED INDEXES
      if (involvedIndexes != null)
        for (String indexName : involvedIndexes) {
          final OIndexAbstract<?> index = (OIndexAbstract<?>) database.getMetadata().getIndexManager().getIndexInternal(indexName);
          if (lockedIndexes == null)
            lockedIndexes = new ArrayList<OIndexAbstract<?>>();

          index.acquireModificationLock();
          lockedIndexes.add(index);
        }

      return lockedIndexes;
    } catch (RuntimeException e) {
      releaseIndexLocks(lockedIndexes);
      throw e;
    }
  }

  private void releaseIndexLocks(List<OIndexAbstract<?>> lockedIndexes) {
    if (lockedIndexes != null) {
      for (OIndexAbstract<?> index : lockedIndexes)
        index.releaseModificationLock();

    }
  }
}
