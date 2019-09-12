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
import com.orientechnologies.orient.core.storage.OBasicTransaction;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.core.tx.OTransactionRealAbstract;

import java.util.*;

/**
 * Created by tglman on 28/12/16.
 */
public class OTransactionOptimisticServer38 extends OTransactionOptimisticServer {
  protected List<ORecordOperationRequest> operations;

  public OTransactionOptimisticServer38(ODatabaseDocumentInternal database, int txId, boolean usingLong,
      List<ORecordOperationRequest> operations, List<IndexChange> indexChanges) {
    super(database, txId, usingLong, indexChanges);
    this.operations = operations;
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
          ORecord record = Orient.instance().getRecordFactoryManager()
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
          if (type == 10) {
            int version = operation.getVersion();
            ORecord updated = database.load(rid);
            if (updated == null) {
              updated = new ODocument();
            }
            ODocumentSerializerDelta delta = new ODocumentSerializerDelta();
            delta.deserializeDelta(operation.getRecord(), (ODocument) updated);
            entry = new ORecordOperation(updated, ORecordOperation.UPDATED);
            ORecordInternal.setIdentity(updated, rid);
            ORecordInternal.setVersion(updated, version);
            updated.setDirty();
            ORecordInternal.setContentChanged(entry.getRecord(), operation.isContentChanged());
            updatedRecords.put(rid, updated);
          } else {
            int version = operation.getVersion();
            ORecord updated = Orient.instance().getRecordFactoryManager()
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
          if (rec == null)
            throw new ORecordNotFoundException(rid.getIdentity());
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
        // SPECIAL CASE FOR UPDATE: WE NEED TO LOAD THE RECORD AND APPLY CHANGES TO GET WORKING HOOKS (LIKE INDEXES)
        final ORecord record = update.record.getRecord();
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
        NavigableMap<Object, OTransactionIndexChangesPerKey> changesPerKey = new TreeMap<>(ODefaultComparator.INSTANCE);
        for (Map.Entry<Object, OTransactionIndexChangesPerKey> keyChange : change.getKeyChanges().changesPerKey.entrySet()) {
          Object key = keyChange.getKey();
          if (key instanceof OIdentifiable && !((OIdentifiable) key).getIdentity().isPersistent())
            key = ((OIdentifiable) key).getRecord();
          if (key instanceof OCompositeKey) {
            key = checkCompositeKeyId((OCompositeKey) key);
          }
          OTransactionIndexChangesPerKey singleChange = new OTransactionIndexChangesPerKey(key);
          for (OTransactionIndexChangesPerKey.OTransactionIndexEntry entry : keyChange.getValue().entries) {
            OIdentifiable rec = entry.value;
            if (rec != null && !rec.getIdentity().isPersistent())
              rec = rec.getRecord();
            singleChange.entries.add(new OTransactionIndexChangesPerKey.OTransactionIndexEntry(rec, entry.operation));
          }
          changesPerKey.put(key, singleChange);
        }
        change.getKeyChanges().changesPerKey = changesPerKey;

        if (change.getKeyChanges().nullKeyChanges != null) {
          OTransactionIndexChangesPerKey singleChange = new OTransactionIndexChangesPerKey(null);
          for (OTransactionIndexChangesPerKey.OTransactionIndexEntry entry : change.getKeyChanges().nullKeyChanges.entries) {
            OIdentifiable rec = entry.value;
            if (rec != null && !rec.getIdentity().isPersistent())
              rec = rec.getRecord();
            singleChange.entries.add(new OTransactionIndexChangesPerKey.OTransactionIndexEntry(rec, entry.operation));
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
      for (ORecord record : updatedRecords.values())
        unmarshallRecord(record);
      oldTxEntries = null;
    } catch (Exception e) {
      rollback();
      throw OException
          .wrapException(new OSerializationException("Cannot read transaction record from the network. Transaction aborted"), e);
    }
  }
}
