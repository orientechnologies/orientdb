package com.orientechnologies.orient.server.tx;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.client.remote.message.tx.IndexChange;
import com.orientechnologies.orient.client.remote.message.tx.ORecordOperationRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.core.tx.OTransactionRealAbstract;

import java.util.*;

/**
 * Created by tglman on 28/12/16.
 */
public class OTransactionOptimisticServer extends OTransactionOptimistic {

  private final Map<ORID, ORecordOperation> tempEntries    = new LinkedHashMap<ORID, ORecordOperation>();
  private final Map<ORecordId, ORecord>     createdRecords = new HashMap<ORecordId, ORecord>();
  private final Map<ORecordId, ORecord>     updatedRecords = new HashMap<ORecordId, ORecord>();
  private final int                           clientTxId;
  private       List<ORecordOperationRequest> operations;
  private final List<IndexChange>             indexChanges;

  public OTransactionOptimisticServer(ODatabaseDocumentInternal database, int txId, boolean usingLong,
      List<ORecordOperationRequest> operations, List<IndexChange> indexChanges) {
    super(database);
    clientTxId = txId;
    this.setUsingLog(usingLong);
    this.operations = operations;
    this.indexChanges = indexChanges;
  }

  @Override
  public void begin() {
    super.begin();
    try {
      for (ORecordOperationRequest operation : this.operations) {
        final byte recordStatus = operation.getType();

        final ORecordId rid = (ORecordId) operation.getId();

        final ORecordOperation entry;

        switch (recordStatus) {
        case ORecordOperation.CREATED:
          entry = new ORecordOperation(operation.getRecord(), ORecordOperation.CREATED);
          ORecordInternal.setIdentity(operation.getRecord(), rid);
          ORecordInternal.setVersion(operation.getRecord(), 0);
          operation.getRecord().setDirty();

          // SAVE THE RECORD TO RETRIEVE THEM FOR THE NEW RID TO SEND BACK TO THE REQUESTER
          createdRecords.put(rid.copy(), entry.getRecord());
          break;

        case ORecordOperation.UPDATED:
          int version = operation.getVersion();
          entry = new ORecordOperation(operation.getRecord(), ORecordOperation.UPDATED);
          ORecordInternal.setIdentity(operation.getRecord(), rid);
          ORecordInternal.setVersion(operation.getRecord(), version);
          operation.getRecord().setDirty();
          ORecordInternal.setContentChanged(entry.getRecord(), operation.isContentChanged());
          break;

        case ORecordOperation.DELETED:
          // LOAD RECORD TO BE SURE IT HASN'T BEEN DELETED BEFORE + PROVIDE CONTENT FOR ANY HOOK
          final ORecord rec = rid.getRecord();
          entry = new ORecordOperation(rec, ORecordOperation.DELETED);
          int deleteVersion = operation.getVersion();
          if (rec == null)
            throw new OConcurrentModificationException(rid.getIdentity(), -1, deleteVersion, ORecordOperation.DELETED);
          else {
            ORecordInternal.setVersion(rec, deleteVersion);
            entry.setRecord(rec);
          }
          break;

        default:
          throw new OTransactionException("Unrecognized tx command: " + recordStatus);
        }

        // PUT IN TEMPORARY LIST TO GET FETCHED AFTER ALL FOR CACHE
        tempEntries.put(entry.getRecord().getIdentity(), entry);
      }
      this.operations = null;

      for (IndexChange change : indexChanges) {
        indexEntries.put(change.getName(), change.getKeyChanges());
      }

      // FIRE THE TRIGGERS ONLY AFTER HAVING PARSED THE REQUEST
      for (Map.Entry<ORID, ORecordOperation> entry : tempEntries.entrySet()) {

        if (entry.getValue().type == ORecordOperation.UPDATED) {
          // SPECIAL CASE FOR UPDATE: WE NEED TO LOAD THE RECORD AND APPLY CHANGES TO GET WORKING HOOKS (LIKE INDEXES)
          final ORecord record = entry.getValue().record.getRecord();
          final boolean contentChanged = ORecordInternal.isContentChanged(record);

          final ORecord loadedRecord = record.getIdentity().copy().getRecord();
          if (loadedRecord == null)
            throw new ORecordNotFoundException(record.getIdentity());

          if (ORecordInternal.getRecordType(loadedRecord) == ODocument.RECORD_TYPE
              && ORecordInternal.getRecordType(loadedRecord) == ORecordInternal.getRecordType(record)) {
            ((ODocument) loadedRecord).merge((ODocument) record, false, false);

            loadedRecord.setDirty();
            ORecordInternal.setContentChanged(loadedRecord, contentChanged);

            ORecordInternal.setVersion(loadedRecord, record.getVersion());
            entry.getValue().record = loadedRecord;

            // SAVE THE RECORD TO RETRIEVE THEM FOR THE NEW VERSIONS TO SEND BACK TO THE REQUESTER
            updatedRecords.put((ORecordId) entry.getKey(), entry.getValue().getRecord());
          }
        }

        addRecord(entry.getValue().getRecord(), entry.getValue().type, null);
      }
      tempEntries.clear();

      // UNMARSHALL ALL THE RECORD AT THE END TO BE SURE ALL THE RECORD ARE LOADED IN LOCAL TX
      for (ORecord record : createdRecords.values()) {
        unmarshallRecord(record);
        if (record instanceof ODocument) {
          // Force conversion of value to class for trigger default values.
          ODocumentInternal.autoConvertValueToClass(getDatabase(), (ODocument) record);
        }
      }
      for (ORecord record : updatedRecords.values())
        unmarshallRecord(record);

    } catch (Exception e) {
      rollback();
      throw OException
          .wrapException(new OSerializationException("Cannot read transaction record from the network. Transaction aborted"), e);
    }
  }

  @Override
  public ORecord getRecord(final ORID rid) {
    ORecord record = super.getRecord(rid);
    if (record == OTransactionRealAbstract.DELETED_RECORD)
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

  /**
   * Unmarshalls collections. This prevent temporary RIDs remains stored as are.
   */
  private void unmarshallRecord(final ORecord iRecord) {
    if (iRecord instanceof ODocument) {
      ((ODocument) iRecord).deserializeFields();
    }
  }
}
