package com.orientechnologies.orient.server.distributed.impl.coordinator.transaction;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.client.remote.message.tx.ORecordOperationRequest;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import com.orientechnologies.orient.server.distributed.impl.coordinator.*;

import java.util.*;

public class OTransactionSubmit implements OSubmitRequest {
  private final OSessionOperationId           operationId;
  private final List<ORecordOperationRequest> operations;
  private final List<OIndexOperationRequest>  indexes;

  public OTransactionSubmit(OSessionOperationId operationId, Collection<ORecordOperation> ops,
      List<OIndexOperationRequest> indexes) {
    this.operationId = operationId;
    this.operations = genOps(ops);
    this.indexes = indexes;
  }

  public static List<ORecordOperationRequest> genOps(Collection<ORecordOperation> ops) {
    List<ORecordOperationRequest> operations = new ArrayList<>();
    for (ORecordOperation txEntry : ops) {
      if (txEntry.type == ORecordOperation.LOADED)
        continue;
      ORecordOperationRequest request = new ORecordOperationRequest();
      request.setType(txEntry.type);
      request.setVersion(txEntry.getRecord().getVersion());
      request.setId(txEntry.getRecord().getIdentity());
      request.setRecordType(ORecordInternal.getRecordType(txEntry.getRecord()));
      switch (txEntry.type) {
      case ORecordOperation.CREATED:
      case ORecordOperation.UPDATED:
        request.setRecord(ORecordSerializerNetworkV37.INSTANCE.toStream(txEntry.getRecord(), false));
        request.setContentChanged(ORecordInternal.isContentChanged(txEntry.getRecord()));
        break;
      case ORecordOperation.DELETED:
        break;
      }
      operations.add(request);
    }
    return operations;
  }

  @Override
  public void begin(ODistributedMember member, ODistributedCoordinator coordinator) {
    ODistributedLockManager lockManager = coordinator.getLockManager();

    //using OPair because there could be different types of values here, so falling back to lexicographic sorting
    Set<OPair<String, String>> keys = new TreeSet<>();
    for (OIndexOperationRequest change : indexes) {
      for (OIndexKeyChange keyChange : change.getIndexKeyChanges()) {
        if (keyChange.getValue() == null) {
          keys.add(new OPair<>(change.getIndexName(), "null"));
        } else {
          keys.add(new OPair<>(change.getIndexName(), keyChange.getValue().toString()));
        }
      }

    }
    for (OPair<String, String> key : keys) {
      lockManager.lockIndexKey(key.getKey(), key.getValue());
    }

    //Sort and lock transaction entry in distributed environment
    Set<ORID> rids = new TreeSet<>();
    Map<ORID, ORID> newIds = new HashMap<>();
    for (ORecordOperationRequest entry : operations) {
      if (ORecordOperation.CREATED == entry.getType()) {
        int clusterId = entry.getId().getClusterId();
        long pos = coordinator.getAllocator().allocate(clusterId);
        newIds.put(entry.getId(), new ORecordId(clusterId, pos));
      } else {
        rids.add(entry.getId());
      }
    }

    List<OLockGuard> guards = new ArrayList<>();
    for (ORID rid : rids) {
      guards.add(lockManager.lockRecord(rid));
    }
    OTransactionFirstPhaseResponseHandler responseHandler = new OTransactionFirstPhaseResponseHandler(operationId, this, member,
        guards);
    coordinator.sendOperation(this, new OTransactionFirstPhaseOperation(this.operationId, this.operations), responseHandler);
  }
}
