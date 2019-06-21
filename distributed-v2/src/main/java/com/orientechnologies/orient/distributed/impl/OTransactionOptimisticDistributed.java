package com.orientechnologies.orient.distributed.impl;

import com.orientechnologies.orient.client.remote.message.tx.ORecordOperationRequest;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.delta.ODocumentDelta;
import com.orientechnologies.orient.core.delta.ODocumentDeltaSerializer;
import com.orientechnologies.orient.core.delta.ODocumentDeltaSerializerI;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
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
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.schedule.OScheduledEvent;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OIndexKeyChange;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OIndexKeyOperation;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OIndexOperationRequest;

import java.util.*;

public class OTransactionOptimisticDistributed extends OTransactionOptimistic {
  private final Map<ORID, ORecord>     createdRecords = new HashMap<>();
  private final Map<ORID, ORecord>     updatedRecords = new HashMap<>();
  private final Set<ORID>              deletedRecord  = new HashSet<>();
  private       List<ORecordOperation> changes;
  private       boolean                useDeltas;

  public OTransactionOptimisticDistributed(ODatabaseDocumentInternal database, List<ORecordOperation> changes, boolean useDeltas) {
    super(database);
    this.changes = changes;
    this.useDeltas = useDeltas;
  }

  public void begin(List<ORecordOperationRequest> operations, List<OIndexOperationRequest> indexes) {
    super.begin();

    List<ORecordOperation> ops = new ArrayList<>();
    for (ORecordOperationRequest req : operations) {
      byte type = req.getType();
      if (type == ORecordOperation.LOADED) {
        continue;
      }

      ORecord record = null;
      switch (type) {
      case ORecordOperation.CREATED: {
        addUpdatedRid(req.getOldId(), req.getId());
        record = ORecordSerializerNetworkV37.INSTANCE.fromStream(req.getRecord(), null, null);
        ORecordInternal.setRecordSerializer(record, database.getSerializer());
        createdRecords.put(req.getOldId(), record);
      }
      break;
      case ORecordOperation.UPDATED: {
        if (useDeltas) {
          ODocumentDeltaSerializerI serializer = ODocumentDeltaSerializer.getActiveSerializer();
          ODocumentDelta updateRecord = serializer.fromStream(new BytesContainer(req.getRecord()));
          ORecordOperation op = new ORecordOperation(updateRecord, type);
          ops.add(op);
        }
        else{
          record = ORecordSerializerNetworkV37.INSTANCE.fromStream(req.getRecord(), null, null);
          ORecordInternal.setRecordSerializer(record, database.getSerializer());
        }
      }
      break;
      case ORecordOperation.DELETED:
        record = database.getRecord(req.getId());
        if (record == null) {
          record = Orient.instance().getRecordFactoryManager()
              .newInstance(req.getRecordType(), req.getId().getClusterId(), database);
        }
        break;
      }
      if (type == ORecordOperation.CREATED || type == ORecordOperation.DELETED || (type == ORecordOperation.UPDATED
          && !useDeltas)) {
        ORecordInternal.setIdentity(record, (ORecordId) req.getId());
        ORecordInternal.setVersion(record, req.getVersion());
        ORecordOperation op = new ORecordOperation(record, type);
        ops.add(op);
      }
    }
    for (ORecordOperation change : ops) {
      allEntries.put(change.getRID(), change);
      resolveTracking(change, false);
    }

    for (OIndexOperationRequest indexChange : indexes) {
      OIndex<?> index = database.getMetadata().getIndexManager().getIndex(indexChange.getIndexName());
      if (indexChange.isCleanIndexValues()) {
        addIndexEntry(index, indexChange.getIndexName(), OTransactionIndexChanges.OPERATION.CLEAR, null, null);
      }
      for (OIndexKeyChange key : indexChange.getIndexKeyChanges()) {
        for (OIndexKeyOperation operation : key.getOperations()) {
          ORID value = operation.getValue();
          if (!value.isPersistent()) {
            value = getRecordEntry(value).getRID();
          }
          if (operation.getType() == OIndexKeyOperation.PUT) {
            addIndexEntry(index, indexChange.getIndexName(), OTransactionIndexChanges.OPERATION.PUT, key.getKey(), value);
          } else {
            addIndexEntry(index, indexChange.getIndexName(), OTransactionIndexChanges.OPERATION.REMOVE, key.getKey(), value);
          }
        }
      }
    }
  }

  @Override
  public void begin() {
    super.begin();
    for (ORecordOperation change : changes) {
      allEntries.put(change.getRID(), change);
      resolveTracking(change, true);
    }

  }

  private void resolveTracking(ORecordOperation change, boolean onlyExecutorCase) {    
    List<OClassIndexManager.IndexChange> changes = new ArrayList<>();
    if (change.getRecordContainer() instanceof ORecord && change.getRecord() instanceof ODocument) {      
      ODocument rec = (ODocument) change.getRecord();
      switch (change.getType()) {
      case ORecordOperation.CREATED: {
          ODocument doc = (ODocument) change.getRecord();
          OLiveQueryHook.addOp(doc, ORecordOperation.CREATED, database);
          OLiveQueryHookV2.addOp(doc, ORecordOperation.CREATED, database);
          OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(doc);
          if (clazz != null) {
            if (onlyExecutorCase) {
              OClassIndexManager.processIndexOnCreate(database, rec, changes);
            }
            if (clazz.isFunction()) {
              database.getSharedContext().getFunctionLibrary().createdFunction(doc);
              Orient.instance().getScriptManager().close(database.getName());
            }
            if (clazz.isSequence()) {
              ((OSequenceLibraryProxy) database.getMetadata().getSequenceLibrary()).getDelegate().onSequenceCreated(database, doc);
            }
            if (clazz.isScheduler()) {
              database.getMetadata().getScheduler().scheduleEvent(new OScheduledEvent(doc));
            }
          }
        }
        if (onlyExecutorCase) {
          createdRecords.put(change.getRID().copy(), change.getRecord());
        }
        break;
      case ORecordOperation.UPDATED: {
          OIdentifiable updateRecord = change.getRecord();                      

          ODocument original = database.load(updateRecord.getIdentity());
          if (original == null){
            throw new ORecordNotFoundException(updateRecord.getIdentity());
          }

          original.merge((ODocument) updateRecord, false, false);          

          ODocument updateDoc = original;
          OLiveQueryHook.addOp(updateDoc, ORecordOperation.UPDATED, database);
          OLiveQueryHookV2.addOp(updateDoc, ORecordOperation.UPDATED, database);
          OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(updateDoc);
          if (clazz != null) {
            if (onlyExecutorCase) {
              OClassIndexManager.processIndexOnUpdate(database, updateDoc, changes);
            }
            if (clazz.isFunction()) {
              database.getSharedContext().getFunctionLibrary().updatedFunction(updateDoc);
              Orient.instance().getScriptManager().close(database.getName());
            }
            if (clazz.isSequence()) {
              ((OSequenceLibraryProxy) database.getMetadata().getSequenceLibrary()).getDelegate()
                  .onSequenceUpdated(database, updateDoc);
            }
          }
        }
        updatedRecords.put(change.getRID(), change.getRecord());
        break;
      case ORecordOperation.DELETED: {        
          ODocument doc = (ODocument) change.getRecord();
          OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(doc);
          if (clazz != null) {
            if (onlyExecutorCase) {
              OClassIndexManager.processIndexOnDelete(database, rec, changes);
            }
            if (clazz.isFunction()) {
              database.getSharedContext().getFunctionLibrary().droppedFunction(doc);
              Orient.instance().getScriptManager().close(database.getName());
            }
            if (clazz.isSequence()) {
              ((OSequenceLibraryProxy) database.getMetadata().getSequenceLibrary()).getDelegate().onSequenceDropped(database, doc);
            }
            if (clazz.isScheduler()) {
              final String eventName = doc.field(OScheduledEvent.PROP_NAME);
              database.getSharedContext().getScheduler().removeEventInternal(eventName);
            }
          }
          OLiveQueryHook.addOp(doc, ORecordOperation.DELETED, database);
          OLiveQueryHookV2.addOp(doc, ORecordOperation.DELETED, database);
        }
        deletedRecord.add(change.getRID());
        break;
      case ORecordOperation.LOADED:
        break;
      default:
        break;
      }
    } else if (change.getRecordContainer() instanceof ODocumentDelta) {      
      switch (change.getType()) {
      case ORecordOperation.UPDATED:

        ODocumentDelta deltaRecord = (ODocumentDelta) change.getRecordContainer();
        ODocument original = database.load(deltaRecord.getIdentity());
        if (original == null)
          throw new ORecordNotFoundException(deltaRecord.getIdentity());
        original = original.mergeDelta(deltaRecord);

        ODocument updateDoc = original;
        OLiveQueryHook.addOp(updateDoc, ORecordOperation.UPDATED, database);
        OLiveQueryHookV2.addOp(updateDoc, ORecordOperation.UPDATED, database);
        OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(updateDoc);
        if (clazz != null) {
          if (onlyExecutorCase) {
            OClassIndexManager.processIndexOnUpdate(database, updateDoc, changes);
          }
          if (clazz.isFunction()) {
            database.getSharedContext().getFunctionLibrary().updatedFunction(updateDoc);
            Orient.instance().getScriptManager().close(database.getName());
          }
          if (clazz.isSequence()) {
            ((OSequenceLibraryProxy) database.getMetadata().getSequenceLibrary()).getDelegate()
                .onSequenceUpdated(database, updateDoc);
          }
        }
        break;
      }
    }
    
    if (onlyExecutorCase) {
      for (OClassIndexManager.IndexChange indexChange : changes) {
        addIndexEntry(indexChange.index, indexChange.index.getName(), indexChange.operation, indexChange.key, indexChange.value);      
      }
    }
  }

  @Override
  public Map<ORID, ORID> getUpdatedRids() {
    return super.getUpdatedRids();
  }

  public Map<ORID, ORecord> getCreatedRecords() {
    return createdRecords;
  }

  public Map<ORID, ORecord> getUpdatedRecords() {
    return updatedRecords;
  }

  public Set<ORID> getDeletedRecord() {
    return deletedRecord;
  }

  public void addUpdatedRid(ORID oldId, ORID id) {
    updatedRids.put(oldId, id);
  }
  
  @Override
  public boolean isUseDeltas(){
    return useDeltas;
  }
}
