package com.orientechnologies.orient.server.distributed.impl.coordinator.transaction;

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.orient.client.remote.message.tx.ORecordOperationRequest;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.distributed.impl.OTransactionOptimisticDistributed;
import com.orientechnologies.orient.server.distributed.impl.coordinator.*;
import com.orientechnologies.orient.server.distributed.impl.coordinator.transaction.OTransactionFirstPhaseResult.*;
import com.orientechnologies.orient.server.distributed.task.ODistributedLockException;

import java.util.ArrayList;
import java.util.List;

public class OTransactionFirstPhaseOperation implements ONodeRequest {

  private final OSessionOperationId           operationId;
  private final List<ORecordOperationRequest> operations;

  public OTransactionFirstPhaseOperation(OSessionOperationId operationId, List<ORecordOperationRequest> operations) {
    this.operationId = operationId;
    this.operations = operations;
  }

  @Override
  public ONodeResponse execute(ODistributedMember nodeFrom, OLogId opId, ODistributedExecutor executor,
      ODatabaseDocumentInternal session) {
    List<ORecordOperation> operations = convert(session, this.operations);
    OTransactionOptimisticDistributed tx = new OTransactionOptimisticDistributed(session, operations);
    ONodeResponse response;
    try {
      ((ODatabaseDocumentDistributed) session).txFirstPhase(operationId, tx);
      response = new OTransactionFirstPhaseResult(Type.SUCCESS, null);

    } catch (OConcurrentModificationException ex) {
      ConcurrentModification metadata = new ConcurrentModification((ORecordId) ex.getRid().getIdentity(),
          ex.getEnhancedRecordVersion(), ex.getEnhancedDatabaseVersion());
      response = new OTransactionFirstPhaseResult(Type.CONCURRENT_MODIFICATION_EXCEPTION, metadata);
    } catch (ORecordDuplicatedException ex) {
      UniqueKeyViolation metadata = new UniqueKeyViolation(ex.getKey().toString(), null, (ORecordId) ex.getRid().getIdentity(),
          ex.getIndexName());
      response = new OTransactionFirstPhaseResult(Type.UNIQUE_KEY_VIOLATION, metadata);
    } catch (RuntimeException ex) {
      //TODO: get action with some exception handler to offline the node or activate a recover operation
      response = new OTransactionFirstPhaseResult(Type.EXCEPTION, null);
    }
    return response;
  }

  private List<ORecordOperation> convert(ODatabaseDocumentInternal database, List<ORecordOperationRequest> operations) {
    List<ORecordOperation> ops = new ArrayList<>();
    for (ORecordOperationRequest req : operations) {
      byte type = req.getType();
      if (type == ORecordOperation.LOADED) {
        continue;
      }

      ORecord record = null;
      switch (type) {
      case ORecordOperation.CREATED:
      case ORecordOperation.UPDATED: {
        record = ORecordSerializerNetworkV37.INSTANCE.fromStream(req.getRecord(), null, null);
        ORecordInternal.setRecordSerializer(record, database.getSerializer());
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
      ORecordInternal.setIdentity(record, (ORecordId) req.getId());
      ORecordInternal.setVersion(record, req.getVersion());
      ORecordOperation op = new ORecordOperation(record, type);
      ops.add(op);
    }
    operations.clear();
    return ops;
  }

}
