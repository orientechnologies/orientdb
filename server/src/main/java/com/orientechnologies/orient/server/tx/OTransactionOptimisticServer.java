package com.orientechnologies.orient.server.tx;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.client.remote.message.tx.IndexChange;
import com.orientechnologies.orient.client.remote.message.tx.ORecordOperationRequest;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OClassIndexManager;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.orientechnologies.orient.core.storage.OBasicTransaction;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.core.tx.OTransactionRealAbstract;

import java.util.*;

/**
 * Created by tglman on 28/12/16.
 */
public abstract class OTransactionOptimisticServer extends OTransactionOptimistic {

  protected final Map<ORID, ORecordOperation> tempEntries    = new LinkedHashMap<ORID, ORecordOperation>();
  protected final Map<ORecordId, ORecord>     createdRecords = new HashMap<ORecordId, ORecord>();
  protected final Map<ORecordId, ORecord>     updatedRecords = new HashMap<ORecordId, ORecord>();
  protected final Set<ORID>                   deletedRecord  = new HashSet<>();
  protected final int                         clientTxId;
  protected final List<IndexChange>           indexChanges;
  protected       Map<ORID, ORecordOperation> oldTxEntries;

  public OTransactionOptimisticServer(ODatabaseDocumentInternal database, int txId, boolean usingLong,
      List<IndexChange> indexChanges) {
    super(database);
    if (database.getTransaction().isActive()) {
      this.newObjectCounter = ((OTransactionRealAbstract) database.getTransaction()).getNewObjectCounter();
    }
    clientTxId = txId;
    this.setUsingLog(usingLong);
    this.indexChanges = indexChanges;
    if (database.getTransaction().isActive()) {
      this.oldTxEntries = new HashMap<>();
      for (ORecordOperation op : ((OTransactionOptimistic) database.getTransaction()).getRecordOperations()) {
        this.oldTxEntries.put(op.getRID(), op);
      }
    }
  }

  @Override
  public int getClientTransactionId() {
    return clientTxId;
  }

  protected OCompositeKey checkCompositeKeyId(OCompositeKey key) {
    OCompositeKey newKey = new OCompositeKey();
    for (Object o : key.getKeys()) {
      if (o instanceof OIdentifiable && !((OIdentifiable) o).getIdentity().isPersistent()) {
        o = ((OIdentifiable) o).getRecord();
      }
      if (o instanceof OCompositeKey)
        o = checkCompositeKeyId((OCompositeKey) o);
      newKey.addKey(o);
    }
    return newKey;
  }

  @Override
  public ORecord getRecord(final ORID rid) {
    ORecord record = super.getRecord(rid);
    if (record == OBasicTransaction.DELETED_RECORD)
      return record;
    else if (record == null && rid.isNew())
      // SEARCH BETWEEN CREATED RECORDS
      record = createdRecords.get(rid);

    return record;
  }

  public Map<ORecordId, ORecord> getCreatedRecords() {
    return createdRecords;
  }

  public Map<ORecordId, ORecord> getUpdatedRecords() {
    return updatedRecords;
  }

  public Set<ORID> getDeletedRecord() {
    return deletedRecord;
  }

  /**
   * Unmarshalls collections. This prevent temporary RIDs remains stored as are.
   */
  protected void unmarshallRecord(final ORecord iRecord) {
    if (iRecord instanceof ODocument) {
      ((ODocument) iRecord).deserializeFields();
    }
  }

  private boolean checkCallHooks(Map<ORID, ORecordOperation> oldTx, ORID id, byte type) {
    if (oldTx != null) {
      ORecordOperation entry = oldTx.get(id);
      if (entry == null || entry.getType() != type)
        return true;
      return false;
    } else {
      return true;
    }
  }

  @Override
  public ORecordOperation addRecord(ORecord iRecord, byte iStatus, String iClusterName) {
    final ORecordOperation operation = super.addRecord(iRecord, iStatus, iClusterName);

    if (iStatus == ORecordOperation.UPDATED) {
      updatedRecords.put((ORecordId) iRecord.getIdentity(), iRecord);
    } else if (iStatus == ORecordOperation.CREATED) {
      createdRecords.put((ORecordId) iRecord.getIdentity().copy(), iRecord);
    } else if (iStatus == ORecordOperation.DELETED) {
      deletedRecord.add(iRecord.getIdentity());
    }

    return operation;
  }

  public void addRecord(ORecord iRecord, final byte iStatus, final String iClusterName, Map<ORID, ORecordOperation> oldTx) {
    changed = true;
    checkTransaction();

    if (iStatus != ORecordOperation.LOADED)
      changedDocuments.remove(iRecord);

    boolean callHooks = checkCallHooks(oldTx, iRecord.getIdentity(), iStatus);

    try {
      if (callHooks) {
        switch (iStatus) {
        case ORecordOperation.CREATED: {
          OIdentifiable res = database.beforeCreateOperations(iRecord, iClusterName);
          if (res != null) {
            iRecord = (ORecord) res;
          }
        }
        break;
        case ORecordOperation.LOADED:
          /**
           * Read hooks already invoked in {@link com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx#executeReadRecord}
           */
          break;
        case ORecordOperation.UPDATED: {
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
      }
      try {
        final ORecordId rid = (ORecordId) iRecord.getIdentity();

        if (!rid.isPersistent() && !rid.isTemporary()) {
          ORecordId oldRid = rid.copy();
          if (rid.getClusterPosition() == ORecordId.CLUSTER_POS_INVALID) {
            ORecordInternal.onBeforeIdentityChanged(iRecord);
            rid.setClusterPosition(newObjectCounter--);
            updatedRids.put(oldRid, rid);
            ORecordInternal.onAfterIdentityChanged(iRecord);
          }
        }

        ORecordOperation txEntry = getRecordEntry(rid);

        if (txEntry == null) {
          // NEW ENTRY: JUST REGISTER IT
          byte status = iStatus;
          if (status == ORecordOperation.UPDATED && iRecord.getIdentity().isTemporary())
            status = ORecordOperation.CREATED;
          txEntry = new ORecordOperation(iRecord, status);
          allEntries.put(rid.copy(), txEntry);
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

        if (callHooks) {
          switch (iStatus) {
          case ORecordOperation.CREATED:
            database.afterCreateOperations(iRecord);
            break;
          case ORecordOperation.LOADED:
            /**
             * Read hooks already invoked in
             * {@link com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx#executeReadRecord} .
             */
            break;
          case ORecordOperation.UPDATED:
            database.afterUpdateOperations(iRecord);
            break;
          case ORecordOperation.DELETED:
            database.afterDeleteOperations(iRecord);
            break;
          }
        } else {
          switch (iStatus) {
          case ORecordOperation.CREATED:
            if (iRecord instanceof ODocument) {
              OClassIndexManager.checkIndexesAfterCreate((ODocument) iRecord, getDatabase());
            }
            break;
          case ORecordOperation.LOADED:
            /**
             * Read hooks already invoked in
             * {@link com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx#executeReadRecord} .
             */
            break;
          case ORecordOperation.UPDATED:
            if (iRecord instanceof ODocument) {
              OClassIndexManager.checkIndexesAfterUpdate((ODocument) iRecord, getDatabase());
            }
            break;
          case ORecordOperation.DELETED:
            if (iRecord instanceof ODocument) {
              OClassIndexManager.checkIndexesAfterDelete((ODocument) iRecord, getDatabase());
            }
            break;
          }
        }
        // RESET TRACKING
        if (iRecord instanceof ODocument && ((ODocument) iRecord).isTrackingChanges()) {
          ODocumentInternal.clearTrackData(((ODocument) iRecord));
        }

      } catch (Exception e) {
        if (callHooks) {
          switch (iStatus) {
          case ORecordOperation.CREATED:
            database.callbackHooks(ORecordHook.TYPE.CREATE_FAILED, iRecord);
            break;
          case ORecordOperation.UPDATED:
            database.callbackHooks(ORecordHook.TYPE.UPDATE_FAILED, iRecord);
            break;
          case ORecordOperation.DELETED:
            database.callbackHooks(ORecordHook.TYPE.DELETE_FAILED, iRecord);
            break;
          }
        }

        throw OException.wrapException(new ODatabaseException("Error on saving record " + iRecord.getIdentity()), e);
      }
    } finally {
      if (callHooks) {
        switch (iStatus) {
        case ORecordOperation.CREATED:
          database.callbackHooks(ORecordHook.TYPE.FINALIZE_CREATION, iRecord);
          break;
        case ORecordOperation.UPDATED:
          database.callbackHooks(ORecordHook.TYPE.FINALIZE_UPDATE, iRecord);
          break;
        case ORecordOperation.DELETED:
          database.callbackHooks(ORecordHook.TYPE.FINALIZE_DELETION, iRecord);
          break;
        }
      }
    }
  }

  public void assignClusters() {
    for (ORecordOperation entry : allEntries.values()) {
      ORecordId rid = (ORecordId) entry.getRID();
      ORecord record = entry.getRecord();
      if (!rid.isPersistent() && !rid.isTemporary()) {
        ORecordId oldRid = rid.copy();
        ORecordInternal.onBeforeIdentityChanged(record);
        database.assignAndCheckCluster(record, null);
        updatedRids.put(rid.copy(), oldRid);
        ORecordInternal.onAfterIdentityChanged(record);
      }

    }
  }

  @Override
  public void addIndexEntry(OIndex<?> delegate, String iIndexName, OTransactionIndexChanges.OPERATION iOperation, Object key,
      OIdentifiable iValue) {
    super.addIndexEntry(delegate, iIndexName, iOperation, key, iValue);
    changed = true;
  }

  @Override
  public void addIndexEntry(OIndex<?> delegate, String iIndexName, OTransactionIndexChanges.OPERATION iOperation, Object key,
      OIdentifiable iValue, boolean clientTrackOnly) {
    super.addIndexEntry(delegate, iIndexName, iOperation, key, iValue, clientTrackOnly);
    changed = true;
  }
}
