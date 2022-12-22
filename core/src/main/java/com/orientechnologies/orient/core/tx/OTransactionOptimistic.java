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

package com.orientechnologies.orient.core.tx;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabase.OPERATION_MODE;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.LatestVersionRecordReader;
import com.orientechnologies.orient.core.db.document.RecordReader;
import com.orientechnologies.orient.core.db.document.SimpleRecordReader;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.hook.ORecordHook.TYPE;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OClassIndexManager;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibraryProxy;
import com.orientechnologies.orient.core.query.live.OLiveQueryHook;
import com.orientechnologies.orient.core.query.live.OLiveQueryHookV2;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODirtyManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.schedule.OScheduledEvent;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class OTransactionOptimistic extends OTransactionRealAbstract {
  private static final AtomicInteger txSerial = new AtomicInteger();
  protected boolean changed = true;
  private boolean alreadyCleared = false;
  private boolean usingLog = true;
  private int txStartCounter;
  private boolean sentToServer = false;

  public OTransactionOptimistic(final ODatabaseDocumentInternal iDatabase) {
    super(iDatabase, txSerial.incrementAndGet());
  }

  public void begin() {
    if (txStartCounter < 0) {
      throw new OTransactionException("Invalid value of TX counter: " + txStartCounter);
    }
    if (txStartCounter == 0) {
      status = TXSTATUS.BEGUN;
    }
    txStartCounter++;

    if (txStartCounter > 1) {
      OLogManager.instance()
          .debug(
              this,
              "Transaction with ID "
                  + getId()
                  + " was already started "
                  + txStartCounter
                  + " times, and will be reused.");
    }
  }

  public void commit() {
    commit(false);
  }

  /**
   * The transaction is reentrant. If {@code begin()} has been called several times, the actual
   * commit happens only after the same amount of {@code commit()} calls
   *
   * @param force commit transaction even
   */
  @Override
  public void commit(final boolean force) {
    checkTransactionValid();
    if (txStartCounter < 0) {
      throw new OStorageException("Invalid value of tx counter: " + txStartCounter);
    }
    if (force) {
      txStartCounter = 0;
    } else {
      txStartCounter--;
    }

    if (txStartCounter == 0) {
      doCommit();
    } else if (txStartCounter > 0) {
      OLogManager.instance()
          .debug(this, "Nested transaction was closed but transaction itself was not committed.");
    } else {
      throw new OTransactionException("Transaction was committed more times than it was started.");
    }
  }

  @Override
  public int amountOfNestedTxs() {
    return txStartCounter;
  }

  public void rollback() {
    rollback(false, -1);
  }

  public void internalRollback() {
    status = TXSTATUS.ROLLBACKING;

    // REMOVE ALL THE DIRTY ENTRIES AND UNDO ANY DIRTY DOCUMENT IF POSSIBLE.
    for (final ORecordOperation v : allEntries.values()) {
      final ORecord rec = v.getRecord();
      rec.unload();
      database.getLocalCache().deleteRecord(rec.getIdentity());
    }
    close();
    status = TXSTATUS.ROLLED_BACK;
  }

  @Override
  public void rollback(boolean force, int commitLevelDiff) {
    if (txStartCounter < 0) {
      throw new OStorageException("Invalid value of TX counter");
    }
    checkTransactionValid();

    txStartCounter += commitLevelDiff;
    status = TXSTATUS.ROLLBACKING;

    if (!force && txStartCounter > 0) {
      OLogManager.instance()
          .debug(
              this,
              "Nested transaction was closed but transaction itself was scheduled for rollback.");
      return;
    }

    if (txStartCounter < 0) {
      throw new OTransactionException(
          "Transaction was rolled back more times than it was started.");
    }

    if (database.isRemote()) {
      final OStorage storage = database.getStorage();
      ((OStorageProxy) storage).rollback(OTransactionOptimistic.this);
    }
    internalRollback();
  }

  public ORecord loadRecord(
      final ORID rid,
      final ORecord iRecord,
      final String fetchPlan,
      final boolean ignoreCache,
      final boolean loadTombstone,
      final OStorage.LOCKING_STRATEGY lockingStrategy) {
    return loadRecord(rid, iRecord, fetchPlan, ignoreCache, true, loadTombstone, lockingStrategy);
  }

  public ORecord loadRecord(
      final ORID rid,
      final ORecord iRecord,
      final String fetchPlan,
      final boolean ignoreCache,
      final boolean iUpdateCache,
      final boolean loadTombstone,
      final OStorage.LOCKING_STRATEGY lockingStrategy) {
    checkTransactionValid();

    final ORecord txRecord = getRecord(rid);
    if (txRecord == OTransactionAbstract.DELETED_RECORD) {
      // DELETED IN TX
      return null;
    }

    if (txRecord != null) {
      if (iRecord != null && txRecord != iRecord) {
        OLogManager.instance()
            .warn(
                this,
                "Found record in transaction with the same RID %s but different instance. "
                    + "Probably the record has been loaded from another transaction and reused on the current one: reload it "
                    + "from current transaction before to update or delete it.",
                iRecord.getIdentity());
      }
      return txRecord;
    }

    if (rid.isTemporary()) {
      return null;
    }

    // DELEGATE TO THE STORAGE, NO TOMBSTONES SUPPORT IN TX MODE
    final ORecord record =
        database.executeReadRecord(
            (ORecordId) rid,
            iRecord,
            -1,
            fetchPlan,
            ignoreCache,
            iUpdateCache,
            loadTombstone,
            lockingStrategy,
            new SimpleRecordReader(database.isPrefetchRecords()));

    if (record != null && isolationLevel == ISOLATION_LEVEL.REPEATABLE_READ) {
      // KEEP THE RECORD IN TX TO ASSURE REPEATABLE READS
      addRecord(record, ORecordOperation.LOADED, null);
    }
    return record;
  }

  @Override
  public ORecord loadRecordIfVersionIsNotLatest(
      ORID rid, final int recordVersion, String fetchPlan, boolean ignoreCache)
      throws ORecordNotFoundException {
    checkTransactionValid();

    final ORecord txRecord = getRecord(rid);
    if (txRecord == OTransactionAbstract.DELETED_RECORD) {
      // DELETED IN TX
      throw new ORecordNotFoundException(rid);
    }

    if (txRecord != null) {
      if (txRecord.getVersion() > recordVersion) {
        return txRecord;
      } else {
        return null;
      }
    }
    if (rid.isTemporary()) {
      throw new ORecordNotFoundException(rid);
    }

    // DELEGATE TO THE STORAGE, NO TOMBSTONES SUPPORT IN TX MODE
    final ORecord record =
        database.executeReadRecord(
            (ORecordId) rid,
            null,
            recordVersion,
            fetchPlan,
            ignoreCache,
            !ignoreCache,
            false,
            OStorage.LOCKING_STRATEGY.NONE,
            new SimpleRecordReader(database.isPrefetchRecords()));

    if (record != null && isolationLevel == ISOLATION_LEVEL.REPEATABLE_READ) {
      // KEEP THE RECORD IN TX TO ASSURE REPEATABLE READS
      addRecord(record, ORecordOperation.LOADED, null);
    }
    return record;
  }

  @Override
  public ORecord reloadRecord(ORID rid, ORecord iRecord, String fetchPlan, boolean ignoreCache) {
    return reloadRecord(rid, iRecord, fetchPlan, ignoreCache, true);
  }

  @Override
  public ORecord reloadRecord(
      ORID rid, ORecord passedRecord, String fetchPlan, boolean ignoreCache, boolean force) {
    checkTransactionValid();

    final ORecord txRecord = getRecord(rid);
    if (txRecord == OTransactionAbstract.DELETED_RECORD) {
      // DELETED IN TX
      return null;
    }

    if (txRecord != null) {
      if (passedRecord != null && txRecord != passedRecord) {
        OLogManager.instance()
            .warn(
                this,
                "Found record in transaction with the same RID %s but different instance. "
                    + "Probably the record has been loaded from another transaction and reused on the current one: reload it "
                    + "from current transaction before to update or delete it",
                passedRecord.getIdentity());
      }
      return txRecord;
    }

    if (rid.isTemporary()) {
      return null;
    }

    // DELEGATE TO THE STORAGE, NO TOMBSTONES SUPPORT IN TX MODE
    final ORecord record;
    try {
      final RecordReader recordReader;
      if (force) {
        recordReader = new SimpleRecordReader(database.isPrefetchRecords());
      } else {
        recordReader = new LatestVersionRecordReader();
      }

      final ORecord loadedRecord =
          database.executeReadRecord(
              (ORecordId) rid,
              passedRecord,
              -1,
              fetchPlan,
              ignoreCache,
              !ignoreCache,
              false,
              OStorage.LOCKING_STRATEGY.NONE,
              recordReader);

      if (force) {
        record = loadedRecord;
      } else {
        if (loadedRecord == null) {
          record = passedRecord;
        } else {
          record = loadedRecord;
        }
      }
    } catch (final ORecordNotFoundException ignore) {
      return null;
    }

    if (record != null && isolationLevel == ISOLATION_LEVEL.REPEATABLE_READ) {
      // KEEP THE RECORD IN TX TO ASSURE REPEATABLE READS
      addRecord(record, ORecordOperation.LOADED, null);
    }
    return record;
  }

  @Override
  public ORecord loadRecord(ORID rid, ORecord record, String fetchPlan, boolean ignoreCache) {
    return loadRecord(rid, record, fetchPlan, ignoreCache, false, OStorage.LOCKING_STRATEGY.NONE);
  }

  public void deleteRecord(final ORecord iRecord, final OPERATION_MODE iMode) {
    if (!iRecord.getIdentity().isValid()) return;
    Set<ORecord> records = ORecordInternal.getDirtyManager(iRecord).getUpdateRecords();
    if (records != null) {
      for (final ORecord rec : records) {
        saveRecord(rec, null, ODatabase.OPERATION_MODE.SYNCHRONOUS, false, null, null);
      }
    }

    final Set<ORecord> newRecords = ORecordInternal.getDirtyManager(iRecord).getNewRecords();
    if (newRecords != null) {
      for (final ORecord rec : newRecords) {
        saveRecord(rec, null, ODatabase.OPERATION_MODE.SYNCHRONOUS, false, null, null);
      }
    }
    addRecord(iRecord, ORecordOperation.DELETED, null);
  }

  public ORecord saveRecord(
      final ORecord iRecord,
      final String iClusterName,
      final OPERATION_MODE iMode,
      final boolean iForceCreate,
      final ORecordCallback<? extends Number> iRecordCreatedCallback,
      final ORecordCallback<Integer> iRecordUpdatedCallback) {
    if (iRecord == null) {
      return null;
    }
    ORecordOperation recordOperation = null;
    boolean originalSaved = false;
    final ODirtyManager dirtyManager = ORecordInternal.getDirtyManager(iRecord);
    do {
      final Set<ORecord> newRecord = dirtyManager.getNewRecords();
      final Set<ORecord> updatedRecord = dirtyManager.getUpdateRecords();
      dirtyManager.clear();
      if (newRecord != null) {
        for (ORecord rec : newRecord) {
          if (rec instanceof ODocument)
            ODocumentInternal.convertAllMultiValuesToTrackedVersions((ODocument) rec);
          if (rec == iRecord) {
            recordOperation = addRecord(rec, ORecordOperation.CREATED, iClusterName);
            originalSaved = true;
          } else addRecord(rec, ORecordOperation.CREATED, database.getClusterName(rec));
        }
      }
      if (updatedRecord != null) {
        for (ORecord rec : updatedRecord) {
          if (rec instanceof ODocument)
            ODocumentInternal.convertAllMultiValuesToTrackedVersions((ODocument) rec);
          if (rec == iRecord) {
            final byte operation;
            if (iForceCreate) {
              operation = ORecordOperation.CREATED;
            } else {
              operation =
                  iRecord.getIdentity().isValid()
                      ? ORecordOperation.UPDATED
                      : ORecordOperation.CREATED;
            }
            recordOperation = addRecord(rec, operation, iClusterName);
            originalSaved = true;
          } else addRecord(rec, ORecordOperation.UPDATED, database.getClusterName(rec));
        }
      }
    } while (dirtyManager.getNewRecords() != null || dirtyManager.getUpdateRecords() != null);

    if (!originalSaved && iRecord.isDirty()) {
      final byte operation;
      if (iForceCreate) {
        operation = ORecordOperation.CREATED;
      } else {
        operation =
            iRecord.getIdentity().isValid() ? ORecordOperation.UPDATED : ORecordOperation.CREATED;
      }
      recordOperation = addRecord(iRecord, operation, iClusterName);
    }
    if (recordOperation != null) {
      if (iRecordCreatedCallback != null)
        //noinspection unchecked
        recordOperation.createdCallback = (ORecordCallback<Long>) iRecordCreatedCallback;
      if (iRecordUpdatedCallback != null) recordOperation.updatedCallback = iRecordUpdatedCallback;
    }
    return iRecord;
  }

  @Override
  public String toString() {
    return "OTransactionOptimistic [id="
        + id
        + ", status="
        + status
        + ", recEntries="
        + allEntries.size()
        + ", idxEntries="
        + indexEntries.size()
        + ']';
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

  public ORecordOperation addRecord(ORecord iRecord, byte iStatus, String iClusterName) {
    changed = true;
    checkTransactionValid();

    if (iClusterName == null) {
      iClusterName = database.getClusterNameById(iRecord.getIdentity().getClusterId());
    }
    if (iStatus != ORecordOperation.LOADED) {
      changedDocuments.remove(iRecord);
    }

    try {
      final ORecordId rid = (ORecordId) iRecord.getIdentity();
      ORecordOperation txEntry = getRecordEntry(rid);
      if (iStatus == ORecordOperation.CREATED && txEntry != null) {
        iStatus = ORecordOperation.UPDATED;
      }
      switch (iStatus) {
        case ORecordOperation.CREATED:
          {
            OIdentifiable res = database.beforeCreateOperations(iRecord, iClusterName);
            if (res != null) {
              iRecord = (ORecord) res;
            }
          }
          break;
        case ORecordOperation.LOADED:
          /** Read hooks already invoked in {@link ODatabaseDocumentTx#executeReadRecord} */
          break;
        case ORecordOperation.UPDATED:
          {
            OIdentifiable res = database.beforeUpdateOperations(iRecord, iClusterName);
            if (res != null) {
              iRecord = (ORecord) res;
            }
          }
          break;
        case ORecordOperation.DELETED:
          database.beforeDeleteOperations(iRecord, iClusterName);
          break;
      }

      try {
        if (!rid.isValid()) {
          ORecordInternal.onBeforeIdentityChanged(iRecord);
          database.assignAndCheckCluster(iRecord, iClusterName);

          rid.setClusterPosition(newObjectCounter--);

          ORecordInternal.onAfterIdentityChanged(iRecord);
        }
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
            database.afterCreateOperations(iRecord);
            break;
          case ORecordOperation.LOADED:
            /** Read hooks already invoked in {@link ODatabaseDocumentTx#executeReadRecord} . */
            break;
          case ORecordOperation.UPDATED:
            database.afterUpdateOperations(iRecord);
            break;
          case ORecordOperation.DELETED:
            database.afterDeleteOperations(iRecord);
            break;
        }

        // RESET TRACKING
        if (iRecord instanceof ODocument && ((ODocument) iRecord).isTrackingChanges()) {
          ODocumentInternal.clearTrackData(((ODocument) iRecord));
        }
        return txEntry;
      } catch (final Exception e) {
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
        throw OException.wrapException(
            new ODatabaseException("Error on saving record " + iRecord.getIdentity()), e);
      }
    } finally {
      switch (iStatus) {
        case ORecordOperation.CREATED:
          database.callbackHooks(TYPE.FINALIZE_CREATION, iRecord);
          break;
        case ORecordOperation.UPDATED:
          database.callbackHooks(TYPE.FINALIZE_UPDATE, iRecord);
          break;
        case ORecordOperation.DELETED:
          database.callbackHooks(TYPE.FINALIZE_DELETION, iRecord);
          break;
      }
    }
  }

  private void doCommit() {
    if (status == TXSTATUS.ROLLED_BACK || status == TXSTATUS.ROLLBACKING) {
      throw new ORollbackException(
          "Given transaction was rolled back, and thus cannot be committed.");
    }
    status = TXSTATUS.COMMITTING;

    if (sentToServer || !allEntries.isEmpty() || !indexEntries.isEmpty()) {
      database.internalCommit(this);
    }
    invokeCallbacks();
    close();
    status = TXSTATUS.COMPLETED;
  }

  private void invokeCallbacks() {
    for (final ORecordOperation recordOperation : allEntries.values()) {
      final ORecord record = recordOperation.getRecord();
      final ORID identity = record.getIdentity();
      locks.keySet();
      if (recordOperation.type == ORecordOperation.CREATED
          && recordOperation.createdCallback != null) {
        recordOperation.createdCallback.call(
            new ORecordId(identity), identity.getClusterPosition());
      } else if (recordOperation.type == ORecordOperation.UPDATED
          && recordOperation.updatedCallback != null) {
        recordOperation.updatedCallback.call(new ORecordId(identity), record.getVersion());
      }
    }
  }

  @Override
  public void addIndexEntry(
      OIndex delegate,
      String iIndexName,
      OTransactionIndexChanges.OPERATION iOperation,
      Object key,
      OIdentifiable iValue,
      boolean clientTrackOnly) {
    changed = true;
    super.addIndexEntry(delegate, iIndexName, iOperation, key, iValue, clientTrackOnly);
  }

  public void resetChangesTracking() {
    alreadyCleared = true;
    changed = false;
  }

  public boolean isChanged() {
    return changed;
  }

  public boolean isAlreadyCleared() {
    return alreadyCleared;
  }

  public Set<ORID> getLockedRecords() {
    if (getNoTxLocks() != null) {
      final Set<ORID> rids = new HashSet<ORID>(getNoTxLocks().keySet());
      rids.addAll(locks.keySet());
      return rids;
    } else {
      return locks.keySet();
    }
  }

  public void setSentToServer(boolean sentToServer) {
    this.sentToServer = sentToServer;
  }

  public boolean getSentToServer() {
    return sentToServer;
  }

  public void fill(final Iterator<ORecordOperation> operations) {
    while (operations.hasNext()) {
      ORecordOperation change = operations.next();
      allEntries.put(change.getRID(), change);
      resolveTracking(change);
    }
  }

  protected void resolveTracking(final ORecordOperation change) {
    final List<OClassIndexManager.IndexChange> changes = new ArrayList<>();
    final ODocument rec = (ODocument) change.getRecord();
    switch (change.getType()) {
      case ORecordOperation.CREATED:
        {
          final ODocument doc = (ODocument) change.getRecord();
          OLiveQueryHook.addOp(doc, ORecordOperation.CREATED, database);
          OLiveQueryHookV2.addOp(doc, ORecordOperation.CREATED, database);
          final OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(doc);
          if (clazz != null) {
            OClassIndexManager.processIndexOnCreate(database, rec, changes);
            if (clazz.isFunction()) {
              database.getSharedContext().getFunctionLibrary().createdFunction(doc);
            }
            if (clazz.isSequence()) {
              ((OSequenceLibraryProxy) database.getMetadata().getSequenceLibrary())
                  .getDelegate()
                  .onSequenceCreated(database, doc);
            }
            if (clazz.isScheduler()) {
              database.getMetadata().getScheduler().scheduleEvent(new OScheduledEvent(doc));
            }
          }
        }
        break;
      case ORecordOperation.UPDATED:
        {
          final OIdentifiable updateRecord = change.getRecord();
          if (updateRecord instanceof ODocument) {
            final ODocument updateDoc = (ODocument) updateRecord;
            OLiveQueryHook.addOp(updateDoc, ORecordOperation.UPDATED, database);
            OLiveQueryHookV2.addOp(updateDoc, ORecordOperation.UPDATED, database);
            final OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(updateDoc);
            if (clazz != null) {
              OClassIndexManager.processIndexOnUpdate(database, updateDoc, changes);
              if (clazz.isFunction()) {
                database.getSharedContext().getFunctionLibrary().updatedFunction(updateDoc);
              }
              if (clazz.isSequence()) {
                ((OSequenceLibraryProxy) database.getMetadata().getSequenceLibrary())
                    .getDelegate()
                    .onSequenceUpdated(database, updateDoc);
              }
            }
          }
        }
        break;
      case ORecordOperation.DELETED:
        {
          final ODocument doc = (ODocument) change.getRecord();
          final OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(doc);
          if (clazz != null) {
            OClassIndexManager.processIndexOnDelete(database, rec, changes);
            if (clazz.isFunction()) {
              database.getSharedContext().getFunctionLibrary().droppedFunction(doc);
              database
                  .getSharedContext()
                  .getOrientDB()
                  .getScriptManager()
                  .close(database.getName());
            }
            if (clazz.isSequence()) {
              ((OSequenceLibraryProxy) database.getMetadata().getSequenceLibrary())
                  .getDelegate()
                  .onSequenceDropped(database, doc);
            }
            if (clazz.isScheduler()) {
              final String eventName = doc.field(OScheduledEvent.PROP_NAME);
              database.getSharedContext().getScheduler().removeEventInternal(eventName);
            }
          }
          OLiveQueryHook.addOp(doc, ORecordOperation.DELETED, database);
          OLiveQueryHookV2.addOp(doc, ORecordOperation.DELETED, database);
        }
        break;
      case ORecordOperation.LOADED:
      default:
        break;
    }

    for (final OClassIndexManager.IndexChange indexChange : changes) {
      addIndexEntry(
          indexChange.index,
          indexChange.index.getName(),
          indexChange.operation,
          indexChange.key,
          indexChange.value);
    }
  }

  protected int getTxStartCounter() {
    return txStartCounter;
  }
}
