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
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ODocumentSerializerDelta;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.orientechnologies.orient.core.tx.*;
import java.util.*;

/** Created by tglman on 28/12/16. */
public class OTransactionOptimisticServer extends OTransactionOptimistic {

  protected final Map<ORID, ORecordOperation> tempEntries =
      new LinkedHashMap<ORID, ORecordOperation>();
  protected final Map<ORecordId, ORecord> createdRecords = new HashMap<ORecordId, ORecord>();
  protected final Map<ORecordId, ORecord> updatedRecords = new HashMap<ORecordId, ORecord>();
  protected final Set<ORID> deletedRecord = new HashSet<>();
  protected final int clientTxId;
  protected final List<IndexChange> indexChanges;
  protected Map<ORID, ORecordOperation> oldTxEntries;
  protected List<ORecordOperationRequest> operations;

  public OTransactionOptimisticServer(
      ODatabaseDocumentInternal database,
      int txId,
      boolean usingLong,
      List<ORecordOperationRequest> operations,
      List<IndexChange> indexChanges) {
    super(database);
    if (database.getTransaction().isActive()) {
      this.newObjectCounter =
          ((OTransactionRealAbstract) database.getTransaction()).getNewObjectCounter();
    }
    clientTxId = txId;
    this.setUsingLog(usingLong);
    this.operations = operations;
    this.indexChanges = indexChanges;
    if (database.getTransaction().isActive()) {
      this.oldTxEntries = new HashMap<>();
      for (ORecordOperation op :
          ((OTransactionOptimistic) database.getTransaction()).getRecordOperations()) {
        this.oldTxEntries.put(op.getRID(), op);
      }
    }
  }

  @Override
  public void begin() {
    super.begin();
    try {
      List<ORecordOperation> toMergeUpdates = new ArrayList<>();
      for (ORecordOperationRequest operation : this.operations) {
        final byte recordStatus = operation.getType();

        final ORecordId rid = (ORecordId) operation.getId();

        final ORecordOperation entry;

        switch (recordStatus) {
          case ORecordOperation.CREATED:
            ORecord record =
                Orient.instance()
                    .getRecordFactoryManager()
                    .newInstance(operation.getRecordType(), rid.getClusterId(), getDatabase());
            ORecordSerializerNetworkV37.INSTANCE.fromStream(operation.getRecord(), record);
            entry = new ORecordOperation(record, ORecordOperation.CREATED);
            ORecordInternal.setIdentity(record, rid);
            ORecordInternal.setVersion(record, 0);
            record.setDirty();

            // SAVE THE RECORD TO RETRIEVE THEM FOR THE NEW RID TO SEND BACK TO THE REQUESTER
            createdRecords.put(rid.copy(), entry.getRecord());
            break;

          case ORecordOperation.UPDATED:
            byte type = operation.getRecordType();
            if (type == ODocumentSerializerDelta.DELTA_RECORD_TYPE) {
              int version = operation.getVersion();
              ORecord updated = database.load(rid);
              if (updated == null) {
                updated = new ODocument();
              }
              ((ODocument) updated).deserializeFields();
              ODocumentInternal.clearTransactionTrackData((ODocument) updated);
              ODocumentSerializerDelta delta = ODocumentSerializerDelta.instance();
              delta.deserializeDelta(operation.getRecord(), (ODocument) updated);
              entry = new ORecordOperation(updated, ORecordOperation.UPDATED);
              ORecordInternal.setIdentity(updated, rid);
              ORecordInternal.setVersion(updated, version);
              updated.setDirty();
              ORecordInternal.setContentChanged(entry.getRecord(), operation.isContentChanged());
              updatedRecords.put(rid, updated);
            } else {
              int version = operation.getVersion();
              ORecord updated =
                  Orient.instance()
                      .getRecordFactoryManager()
                      .newInstance(operation.getRecordType(), rid.getClusterId(), getDatabase());
              ORecordSerializerNetworkV37.INSTANCE.fromStream(operation.getRecord(), updated);
              entry = new ORecordOperation(updated, ORecordOperation.UPDATED);
              ORecordInternal.setIdentity(updated, rid);
              ORecordInternal.setVersion(updated, version);
              updated.setDirty();
              ORecordInternal.setContentChanged(entry.getRecord(), operation.isContentChanged());
              toMergeUpdates.add(entry);
            }
            break;

          case ORecordOperation.DELETED:
            // LOAD RECORD TO BE SURE IT HASN'T BEEN DELETED BEFORE + PROVIDE CONTENT FOR ANY HOOK
            final ORecord rec = rid.getRecord();
            entry = new ORecordOperation(rec, ORecordOperation.DELETED);
            int deleteVersion = operation.getVersion();
            if (rec == null) throw new ORecordNotFoundException(rid.getIdentity());
            else {
              ORecordInternal.setVersion(rec, deleteVersion);
              entry.setRecord(rec);
            }
            deletedRecord.add(rec.getIdentity());
            break;

          default:
            throw new OTransactionException("Unrecognized tx command: " + recordStatus);
        }

        // PUT IN TEMPORARY LIST TO GET FETCHED AFTER ALL FOR CACHE
        tempEntries.put(entry.getRecord().getIdentity(), entry);
      }
      this.operations = null;
      for (ORecordOperation update : toMergeUpdates) {
        // SPECIAL CASE FOR UPDATE: WE NEED TO LOAD THE RECORD AND APPLY CHANGES TO GET WORKING
        // HOOKS (LIKE INDEXES)
        final ORecord record = update.record.getRecord();
        final boolean contentChanged = ORecordInternal.isContentChanged(record);

        final ORecord loadedRecord = record.getIdentity().copy().getRecord();
        if (loadedRecord == null) throw new ORecordNotFoundException(record.getIdentity());

        if (ORecordInternal.getRecordType(loadedRecord) == ODocument.RECORD_TYPE
            && ORecordInternal.getRecordType(loadedRecord)
                == ORecordInternal.getRecordType(record)) {
          ((ODocument) loadedRecord).merge((ODocument) record, false, false);

          loadedRecord.setDirty();
          ORecordInternal.setContentChanged(loadedRecord, contentChanged);

          ORecordInternal.setVersion(loadedRecord, record.getVersion());
          update.record = loadedRecord;

          // SAVE THE RECORD TO RETRIEVE THEM FOR THE NEW VERSIONS TO SEND BACK TO THE REQUESTER
          updatedRecords.put((ORecordId) update.getRID(), update.getRecord());
        }
      }

      // FIRE THE TRIGGERS ONLY AFTER HAVING PARSED THE REQUEST
      for (Map.Entry<ORID, ORecordOperation> entry : tempEntries.entrySet()) {
        database.getLocalCache().updateRecord(entry.getValue().getRecord());
        addRecord(entry.getValue().getRecord(), entry.getValue().type, null, oldTxEntries);
      }
      tempEntries.clear();

      for (IndexChange change : indexChanges) {
        NavigableMap<Object, OTransactionIndexChangesPerKey> changesPerKey =
            new TreeMap<>(ODefaultComparator.INSTANCE);
        for (Map.Entry<Object, OTransactionIndexChangesPerKey> keyChange :
            change.getKeyChanges().changesPerKey.entrySet()) {
          Object key = keyChange.getKey();
          if (key instanceof OIdentifiable && !((OIdentifiable) key).getIdentity().isPersistent())
            key = ((OIdentifiable) key).getRecord();
          if (key instanceof OCompositeKey) {
            key = checkCompositeKeyId((OCompositeKey) key);
          }
          OTransactionIndexChangesPerKey singleChange = new OTransactionIndexChangesPerKey(key);
          for (OTransactionIndexChangesPerKey.OTransactionIndexEntry entry :
              keyChange.getValue().getEntriesAsList()) {
            OIdentifiable rec = entry.getValue();
            if (rec != null && !rec.getIdentity().isPersistent()) rec = rec.getRecord();
            singleChange.add(rec, entry.getOperation());
          }
          changesPerKey.put(key, singleChange);
        }
        change.getKeyChanges().changesPerKey = changesPerKey;

        if (change.getKeyChanges().nullKeyChanges != null) {
          OTransactionIndexChangesPerKey singleChange = new OTransactionIndexChangesPerKey(null);
          for (OTransactionIndexChangesPerKey.OTransactionIndexEntry entry :
              change.getKeyChanges().nullKeyChanges.getEntriesAsList()) {
            OIdentifiable rec = entry.getValue();
            if (rec != null && !rec.getIdentity().isPersistent()) rec = rec.getRecord();
            singleChange.add(rec, entry.getOperation());
          }
          change.getKeyChanges().nullKeyChanges = singleChange;
        }
        indexEntries.put(change.getName(), change.getKeyChanges());
      }
      newObjectCounter = (createdRecords.size() + 2) * -1;
      // UNMARSHALL ALL THE RECORD AT THE END TO BE SURE ALL THE RECORD ARE LOADED IN LOCAL TX
      for (ORecord record : createdRecords.values()) {
        unmarshallRecord(record);
        if (record instanceof ODocument) {
          // Force conversion of value to class for trigger default values.
          ODocumentInternal.autoConvertValueToClass(getDatabase(), (ODocument) record);
        }
      }
      for (ORecord record : updatedRecords.values()) unmarshallRecord(record);
      oldTxEntries = null;
    } catch (Exception e) {
      rollback();
      throw OException.wrapException(
          new OSerializationException(
              "Cannot read transaction record from the network. Transaction aborted"),
          e);
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
      if (o instanceof OCompositeKey) o = checkCompositeKeyId((OCompositeKey) o);
      newKey.addKey(o);
    }
    return newKey;
  }

  @Override
  public ORecord getRecord(final ORID rid) {
    ORecord record = super.getRecord(rid);
    if (record == OTransactionAbstract.DELETED_RECORD) return record;
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

  /** Unmarshalls collections. This prevent temporary RIDs remains stored as are. */
  protected void unmarshallRecord(final ORecord iRecord) {
    if (iRecord instanceof ODocument) {
      ((ODocument) iRecord).deserializeFields();
    }
  }

  private boolean checkCallHooks(Map<ORID, ORecordOperation> oldTx, ORID id, byte type) {
    if (oldTx != null) {
      ORecordOperation entry = oldTx.get(id);
      return entry == null || entry.getType() != type;
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

  public void addRecord(
      ORecord iRecord,
      final byte iStatus,
      final String iClusterName,
      Map<ORID, ORecordOperation> oldTx) {
    changed = true;
    checkTransactionValid();

    if (iStatus != ORecordOperation.LOADED) changedDocuments.remove(iRecord);

    boolean callHooks = checkCallHooks(oldTx, iRecord.getIdentity(), iStatus);

    try {
      if (callHooks) {
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
            /**
             * Read hooks already invoked in {@link
             * com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx#executeReadRecord}
             */
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
               * Read hooks already invoked in {@link
               * com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx#executeReadRecord}
               * .
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
               * Read hooks already invoked in {@link
               * com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx#executeReadRecord}
               * .
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

        throw OException.wrapException(
            new ODatabaseException("Error on saving record " + iRecord.getIdentity()), e);
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
  public void addIndexEntry(
      OIndex delegate,
      String iIndexName,
      OTransactionIndexChanges.OPERATION iOperation,
      Object key,
      OIdentifiable iValue) {
    super.addIndexEntry(delegate, iIndexName, iOperation, key, iValue);
    changed = true;
  }

  @Override
  public void addIndexEntry(
      OIndex delegate,
      String iIndexName,
      OTransactionIndexChanges.OPERATION iOperation,
      Object key,
      OIdentifiable iValue,
      boolean clientTrackOnly) {
    super.addIndexEntry(delegate, iIndexName, iOperation, key, iValue, clientTrackOnly);
    changed = true;
  }
}
