package com.orientechnologies.orient.distributed.impl.coordinator.transaction;

import static com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory.TRANSACTION_SUBMIT_REQUEST;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.client.remote.message.tx.ORecordOperationRequest;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkDistributed;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedCoordinator;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedLockManager;
import com.orientechnologies.orient.distributed.impl.coordinator.OSubmitRequest;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

public class OTransactionSubmit implements OSubmitRequest {
  private List<ORecordOperationRequest> operations;
  private List<OIndexOperationRequest> indexes;

  private static final String _sequencesBaseClass = "OSequence";

  public OTransactionSubmit(
      Collection<ORecordOperation> ops, List<OIndexOperationRequest> indexes) {
    this.operations = genOps(ops);
    this.indexes = indexes;
  }

  public OTransactionSubmit() {}

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

  public static List<ORecordOperationRequest> genOps(Collection<ORecordOperation> ops) {
    List<ORecordOperationRequest> operations = new ArrayList<>();
    for (ORecordOperation txEntry : ops) {
      if (txEntry.type == ORecordOperation.LOADED) continue;
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
          request.setRecord(
              ORecordSerializerNetworkDistributed.INSTANCE.toStream(txEntry.getRecord()));
          request.setContentChanged(ORecordInternal.isContentChanged(txEntry.getRecord()));
          break;
        case ORecordOperation.UPDATED:
          request.setRecord(
              ORecordSerializerNetworkDistributed.INSTANCE.toStream(txEntry.getRecord()));
          request.setContentChanged(ORecordInternal.isContentChanged(txEntry.getRecord()));
          break;
        case ORecordOperation.DELETED:
          break;
      }
      operations.add(request);
    }
    return operations;
  }

  public static List<OIndexOperationRequest> genIndexes(
      Map<String, OTransactionIndexChanges> indexOperations, OTransactionInternal tx) {
    List<OIndexOperationRequest> idx = new ArrayList<>();
    for (Map.Entry<String, OTransactionIndexChanges> index : indexOperations.entrySet()) {
      OTransactionIndexChanges changes = index.getValue();
      List<OIndexKeyChange> keys = new ArrayList<>();
      for (Map.Entry<Object, OTransactionIndexChangesPerKey> entry :
          changes.changesPerKey.entrySet()) {
        List<OIndexKeyOperation> oper = new ArrayList<>();
        for (OTransactionIndexChangesPerKey.OTransactionIndexEntry operat :
            entry.getValue().entries) {
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
        for (OTransactionIndexChangesPerKey.OTransactionIndexEntry operat :
            index.getValue().nullKeyChanges.entries) {
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
  public void begin(
      ONodeIdentity requester,
      OSessionOperationId operationId,
      ODistributedCoordinator coordinator) {
    ODistributedLockManager lockManager = coordinator.getLockManager();

    // using OPair because there could be different types of values here, so falling back to
    // lexicographic sorting
    SortedSet<OPair<String, String>> keys = new TreeSet<>();
    for (OIndexOperationRequest change : indexes) {
      for (OIndexKeyChange keyChange : change.getIndexKeyChanges()) {
        if (keyChange.getKey() == null) {
          keys.add(new OPair<>(change.getIndexName(), "null"));
        } else {
          keys.add(new OPair<>(change.getIndexName(), keyChange.getKey().toString()));
        }
      }
    }

    // Sort and lock transaction entry in distributed environment
    SortedSet<ORID> rids = new TreeSet<>();
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
    lockManager.lock(
        rids,
        keys,
        (guards) -> {
          OTransactionFirstPhaseResponseHandler responseHandler =
              new OTransactionFirstPhaseResponseHandler(
                  operationId, this, requester, operations, indexes, guards);
          OTransactionFirstPhaseOperation request =
              new OTransactionFirstPhaseOperation(operationId, this.operations, indexes);
          coordinator.sendOperation(this, request, responseHandler);
        });
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

  public List<OIndexOperationRequest> getIndexes() {
    return indexes;
  }

  public List<ORecordOperationRequest> getOperations() {
    return operations;
  }

  @Override
  public int getRequestType() {
    return TRANSACTION_SUBMIT_REQUEST;
  }

  public boolean isEmpty() {
    return operations.isEmpty();
  }
}
