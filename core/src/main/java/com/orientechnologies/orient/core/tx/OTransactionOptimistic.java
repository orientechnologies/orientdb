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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.OUncompletedCommit;
import com.orientechnologies.orient.core.db.ODatabase.OPERATION_MODE;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.engine.local.OEngineLocalPaginated;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.hook.ORecordHook.RESULT;
import com.orientechnologies.orient.core.hook.ORecordHook.TYPE;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODirtyManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public class OTransactionOptimistic extends OTransactionRealAbstract {
  private static AtomicInteger txSerial = new AtomicInteger();

  private boolean              usingLog = true;
  private int                  txStartCounter;

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
  public OUncompletedCommit<Void> initiateCommit() {
    return initiateCommit(false);
  }

  @Override
  public OUncompletedCommit<Void> initiateCommit(boolean force) {
    checkTransaction();

    if (txStartCounter < 0)
      throw new OStorageException("Invalid value of tx counter");

    if (force)
      txStartCounter = 0;
    else
      txStartCounter--;

    if (txStartCounter == 0)
      return doInitiateCommit();
    else if (txStartCounter > 0)
      return new UncompletedCommit(false, null);
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

    // REMOVE ALL THE DIRTY ENTRIES AND UNDO ANY DIRTY DOCUMENT IF POSSIBLE.
    for (ORecordOperation v : allEntries.values()) {
      final ORecord rec = v.getRecord();
      if (rec.isDirty())
        if (rec instanceof ODocument && ((ODocument) rec).isTrackingChanges())
          ((ODocument) rec).undo();
        else
          rec.unload();
    }

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
    final ORecord record = database.executeReadRecord((ORecordId) rid, iRecord, -1, fetchPlan, ignoreCache, iUpdateCache,
        loadTombstone, lockingStrategy, new ODatabaseDocumentTx.SimpleRecordReader());

    if (record != null && isolationLevel == ISOLATION_LEVEL.REPEATABLE_READ)
      // KEEP THE RECORD IN TX TO ASSURE REPEATABLE READS
      addRecord(record, ORecordOperation.LOADED, null);

    return record;
  }

  @Override
  public ORecord loadRecordIfVersionIsNotLatest(ORID rid, final int recordVersion, String fetchPlan, boolean ignoreCache)
      throws ORecordNotFoundException {
    checkTransaction();

    final ORecord txRecord = getRecord(rid);
    if (txRecord == OTransactionRealAbstract.DELETED_RECORD)
      // DELETED IN TX
      throw new ORecordNotFoundException(rid);

    if (txRecord != null) {
      if (txRecord.getVersion() > recordVersion)
        return txRecord;
      else
        return null;
    }

    if (rid.isTemporary())
      throw new ORecordNotFoundException(rid);

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

      ORecord loadedRecord = database.executeReadRecord((ORecordId) rid, passedRecord, -1, fetchPlan, ignoreCache, !ignoreCache,
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

  public ORecord saveRecord(final ORecord iRecord, final String iClusterName, final OPERATION_MODE iMode,
      final boolean iForceCreate, final ORecordCallback<? extends Number> iRecordCreatedCallback,
      final ORecordCallback<Integer> iRecordUpdatedCallback) {
    if (iRecord == null)
      return null;

    boolean originalSaved = false;
    final ODirtyManager dirtyManager = ORecordInternal.getDirtyManager(iRecord);
    do {
      Set<ORecord> newRecord = dirtyManager.getNewRecords();
      Set<ORecord> updatedRecord = dirtyManager.getUpdateRecords();
      dirtyManager.clear();
      if (newRecord != null) {
        for (ORecord rec : newRecord) {
          if (rec instanceof ODocument)
            ODocumentInternal.convertAllMultiValuesToTrackedVersions((ODocument) rec);
          if (rec == iRecord) {
            addRecord(rec, ORecordOperation.CREATED, iClusterName);
            originalSaved = true;
          } else
            addRecord(rec, ORecordOperation.CREATED, getClusterName(rec));
        }
      }
      if (updatedRecord != null) {
        for (ORecord rec : updatedRecord) {
          if (rec instanceof ODocument)
            ODocumentInternal.convertAllMultiValuesToTrackedVersions((ODocument) rec);
          if (rec == iRecord) {
            final byte operation = iForceCreate ? ORecordOperation.CREATED
                : iRecord.getIdentity().isValid() ? ORecordOperation.UPDATED : ORecordOperation.CREATED;
            addRecord(rec, operation, iClusterName);
            originalSaved = true;
          } else
            addRecord(rec, ORecordOperation.UPDATED, getClusterName(rec));
        }
      }
    } while (dirtyManager.getNewRecords() != null || dirtyManager.getUpdateRecords() != null);

    if (!originalSaved && iRecord.isDirty()) {
      final byte operation = iForceCreate ? ORecordOperation.CREATED
          : iRecord.getIdentity().isValid() ? ORecordOperation.UPDATED : ORecordOperation.CREATED;
      addRecord(iRecord, operation, iClusterName);
    }
    return iRecord;
  }

  @Override
  public String toString() {
    return "OTransactionOptimistic [id=" + id + ", status=" + status + ", recEntries=" + allEntries.size() + ", idxEntries="
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
        final ORecordId rid = (ORecordId) iRecord.getIdentity();

        if (!rid.isValid()) {
          ORecordInternal.onBeforeIdentityChanged(iRecord);
          database.assignAndCheckCluster(iRecord, iClusterName);

          rid.clusterPosition = newObjectCounter--;

          ORecordInternal.onAfterIdentityChanged(iRecord);
        }

        ORecordOperation txEntry = getRecordEntry(rid);

        if (txEntry == null) {
          if (!(rid.isTemporary() && iStatus != ORecordOperation.CREATED)) {
            // NEW ENTRY: JUST REGISTER IT
            txEntry = new ORecordOperation(iRecord, iStatus);
            allEntries.put(rid.copy(), txEntry);
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
              allEntries.remove(rid);
              // txEntry.type = ORecordOperation.DELETED;
              break;
            }
            break;
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
          ODocumentInternal.clearTrackData(((ODocument) iRecord));
        }

      } catch (Exception e) {
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

        throw OException.wrapException(new ODatabaseException("Error on saving record " + iRecord.getIdentity()), e);
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

    if (!allEntries.isEmpty() || !indexEntries.isEmpty()) {
      if (!OScenarioThreadLocal.INSTANCE.isRunModeDistributed()
          && !(database.getStorage().getUnderlying() instanceof OAbstractPaginatedStorage))
        database.getStorage().commit(this, null);
      else {

        final String storageType = database.getStorage().getUnderlying().getType();

        if (storageType.equals(OEngineLocalPaginated.NAME) || storageType.equals(OEngineMemory.NAME))
          database.getStorage().commit(OTransactionOptimistic.this, null);
        else {
          database.getStorage().callInLock(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
              database.getStorage().commit(OTransactionOptimistic.this, null);
              return null;
            }
          }, true);
        }
      }
    }

    close();

    status = TXSTATUS.COMPLETED;
  }

  private OUncompletedCommit<Void> doInitiateCommit() {
    if (status == TXSTATUS.ROLLED_BACK || status == TXSTATUS.ROLLBACKING)
      throw new ORollbackException("Given transaction was rolled back and cannot be used.");

    status = TXSTATUS.COMMITTING;

    if (!allEntries.isEmpty() || !indexEntries.isEmpty()) {
      if (!OScenarioThreadLocal.INSTANCE.isRunModeDistributed()
          && !(database.getStorage().getUnderlying() instanceof OAbstractPaginatedStorage))
        return new UncompletedCommit(true, database.getStorage().initiateCommit(this, null));
      else {
        final String storageType = database.getStorage().getUnderlying().getType();

        if (storageType.equals(OEngineLocalPaginated.NAME) || storageType.equals(OEngineMemory.NAME))
          return new UncompletedCommit(true, database.getStorage().initiateCommit(OTransactionOptimistic.this, null));
        else
          return database.getStorage().callInLock(new Callable<OUncompletedCommit<Void>>() {
            @Override
            public OUncompletedCommit<Void> call() throws Exception {
              return new UncompletedCommit(true, database.getStorage().initiateCommit(OTransactionOptimistic.this, null));
            }
          }, true);
      }
    }

    return new UncompletedCommit(true, null);
  }

  private class UncompletedCommit implements OUncompletedCommit<Void> {

    private final boolean                                    topLevel;
    private final OUncompletedCommit<List<ORecordOperation>> nestedCommit;

    public UncompletedCommit(boolean topLevel, OUncompletedCommit<List<ORecordOperation>> nestedCommit) {
      this.topLevel = topLevel;
      this.nestedCommit = nestedCommit;
    }

    @Override
    public Void complete() {
      checkTransaction();

      if (!topLevel) {
        OLogManager.instance().debug(this, "Nested transaction was closed but transaction itself was not committed.");
        return null;
      }

      if (nestedCommit != null)
        nestedCommit.complete();

      close();
      status = TXSTATUS.COMPLETED;

      return null;
    }

    @Override
    public void rollback() {
      checkTransaction();

      if (!topLevel) {
        OTransactionOptimistic.this.rollback(false, 0);
        return;
      }

      status = TXSTATUS.ROLLBACKING;

      if (nestedCommit != null)
        nestedCommit.rollback();

      // CLEAR THE CACHE
      database.getLocalCache().clear();

      // REMOVE ALL THE DIRTY ENTRIES AND UNDO ANY DIRTY DOCUMENT IF POSSIBLE.
      for (ORecordOperation v : allEntries.values()) {
        final ORecord rec = v.getRecord();
        if (rec.isDirty())
          if (rec instanceof ODocument && ((ODocument) rec).isTrackingChanges())
            ((ODocument) rec).undo();
          else
            rec.unload();
      }

      close();

      status = TXSTATUS.ROLLED_BACK;
    }
  }

}
