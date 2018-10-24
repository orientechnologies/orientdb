package com.orientechnologies.orient.server.distributed.impl.coordinator.transaction;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.client.remote.message.tx.ORecordOperationRequest;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.server.distributed.impl.coordinator.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import static com.orientechnologies.orient.server.distributed.impl.coordinator.OCoordinateMessagesFactory.TRANSACTION_SUBMIT_REQUEST;

public class OTransactionSubmit implements OSubmitRequest {
  private List<ORecordOperationRequest> operations;
  private List<OIndexOperationRequest>  indexes;

  private static final String _sequencesBaseClass = "OSequence";

  public OTransactionSubmit(Collection<ORecordOperation> ops, List<OIndexOperationRequest> indexes, boolean useDeltas) {
    this.operations = genOps(ops, useDeltas);
    this.indexes = indexes;
  }

  public OTransactionSubmit() {

  }

  private static boolean isSequenceDocument(ORecordOperation txEntry) {
    if (txEntry.record != null && txEntry.record.getRecord() instanceof ODocument) {
      ODocument doc = txEntry.record.getRecord();
      OClass docClass = doc.getSchemaClass();
      if (docClass != null && docClass.isSubClassOf(OSequence.CLASS_NAME)) {
        return true;
      }
    }
    return false;
  }

  public static List<ORecordOperationRequest> genOps(Collection<ORecordOperation> ops, boolean useDeltas) {
    List<ORecordOperationRequest> operations = new ArrayList<>();
    for (ORecordOperation txEntry : ops) {
      if (txEntry.type == ORecordOperation.LOADED)
        continue;
      if (isSequenceDocument(txEntry)) {
        continue;
      }
      ORecordOperationRequest request = new ORecordOperationRequest();
      request.setType(txEntry.type);
      request.setVersion(txEntry.getRecord().getVersion());
      request.setId(txEntry.getRecord().getIdentity());
      request.setRecordType(ORecordInternal.getRecordType(txEntry.getRecord()));
      switch (txEntry.type) {
      case ORecordOperation.CREATED:
        request.setRecord(ORecordSerializerNetworkV37.INSTANCE.toStream(txEntry.getRecord(), false));
        request.setContentChanged(ORecordInternal.isContentChanged(txEntry.getRecord()));
        break;
      case ORecordOperation.UPDATED:
        request.setRecord(ORecordSerializerNetworkV37.INSTANCE.toStream(txEntry.getRecord(), useDeltas));
        request.setContentChanged(ORecordInternal.isContentChanged(txEntry.getRecord()));
        break;
      case ORecordOperation.DELETED:
        break;
      }
      operations.add(request);
    }
    return operations;
  }

  public static List<OIndexOperationRequest> genIndexes(Map<String, OTransactionIndexChanges> indexOperations,
      OTransactionInternal tx) {
    List<OIndexOperationRequest> idx = new ArrayList<>();
    for (Map.Entry<String, OTransactionIndexChanges> index : indexOperations.entrySet()) {
      OTransactionIndexChanges changes = index.getValue();
      List<OIndexKeyChange> keys = new ArrayList<>();
      for (Map.Entry<Object, OTransactionIndexChangesPerKey> entry : changes.changesPerKey.entrySet()) {
        List<OIndexKeyOperation> oper = new ArrayList<>();
        for (OTransactionIndexChangesPerKey.OTransactionIndexEntry operat : entry.getValue().entries) {
          ORID identity = operat.value.getIdentity();
          if (!identity.isPersistent()) {
            identity = tx.getRecordEntry(identity).getRID();
          }
          if (operat.operation == OTransactionIndexChanges.OPERATION.PUT) {
            oper.add(new OIndexKeyOperation(OIndexKeyOperation.PUT, identity));
          } else {
            oper.add(new OIndexKeyOperation(OIndexKeyOperation.REMOVE, identity));
          }
        }
        keys.add(new OIndexKeyChange(entry.getKey(), oper));
      }
      if (index.getValue().nullKeyChanges != null) {
        List<OIndexKeyOperation> oper = new ArrayList<>();
        for (OTransactionIndexChangesPerKey.OTransactionIndexEntry operat : index.getValue().nullKeyChanges.entries) {
          ORID identity = operat.value.getIdentity();
          if (!identity.isPersistent()) {
            identity = tx.getRecordEntry(identity).getRID();
          }
          if (operat.operation == OTransactionIndexChanges.OPERATION.PUT) {
            oper.add(new OIndexKeyOperation(OIndexKeyOperation.PUT, identity));
          } else {
            oper.add(new OIndexKeyOperation(OIndexKeyOperation.REMOVE, identity));
          }
        }
        keys.add(new OIndexKeyChange(null, oper));

      }
      idx.add(new OIndexOperationRequest(index.getKey(), changes.cleared, keys));
    }
    return idx;
  }

  @Override
  public void begin(ODistributedMember requester, OSessionOperationId operationId, ODistributedCoordinator coordinator) {
    ODistributedLockManager lockManager = coordinator.getLockManager();

    //using OPair because there could be different types of values here, so falling back to lexicographic sorting
    Set<OPair<String, String>> keys = new TreeSet<>();
    for (OIndexOperationRequest change : indexes) {
      for (OIndexKeyChange keyChange : change.getIndexKeyChanges()) {
        if (keyChange.getKey() == null) {
          keys.add(new OPair<>(change.getIndexName(), "null"));
        } else {
          keys.add(new OPair<>(change.getIndexName(), keyChange.getKey().toString()));
        }
      }

    }
    List<OLockGuard> guards = new ArrayList<>();
    for (OPair<String, String> key : keys) {
      guards.add(lockManager.lockIndexKey(key.getKey(), key.getValue()));
    }

    //Sort and lock transaction entry in distributed environment
    Set<ORID> rids = new TreeSet<>();
    for (ORecordOperationRequest entry : operations) {
      if (ORecordOperation.CREATED == entry.getType()) {
        int clusterId = entry.getId().getClusterId();
        long pos = coordinator.getAllocator().allocate(clusterId);
        ORecordId value = new ORecordId(clusterId, pos);
        entry.setOldId(entry.getId());
        entry.setId(value);
      } else {
        rids.add(entry.getId());
      }
    }

    for (ORID rid : rids) {
      guards.add(lockManager.lockRecord(rid));
    }
    OTransactionFirstPhaseResponseHandler responseHandler = new OTransactionFirstPhaseResponseHandler(operationId, this, requester,
        guards);
    OTransactionFirstPhaseOperation request = new OTransactionFirstPhaseOperation(operationId, this.operations, indexes);
    coordinator.sendOperation(this, request, responseHandler);
  }

  @Override
  public void deserialize(DataInput input) throws IOException {

    int size = input.readInt();
    operations = new ArrayList<>(size);
    while (size-- > 0) {
      ORecordOperationRequest op = new ORecordOperationRequest();
      op.deserialize(input);
      operations.add(op);
    }

    size = input.readInt();
    indexes = new ArrayList<>(size);
    while (size-- > 0) {
      OIndexOperationRequest change = new OIndexOperationRequest();
      change.deserialize(input);
      indexes.add(change);
    }

  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    output.writeInt(operations.size());
    for (ORecordOperationRequest operation : operations) {
      operation.serialize(output);
    }
    output.writeInt(indexes.size());
    for (OIndexOperationRequest change : indexes) {
      change.serialize(output);
    }
  }

  @Override
  public int getRequestType() {
    return TRANSACTION_SUBMIT_REQUEST;
  }

  public boolean isEmpty() {
    return operations.isEmpty();
  }
}
