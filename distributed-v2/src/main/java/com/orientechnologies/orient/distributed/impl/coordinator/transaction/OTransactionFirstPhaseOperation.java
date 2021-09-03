package com.orientechnologies.orient.distributed.impl.coordinator.transaction;

import static com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory.TRANSACTION_FIRST_PHASE_REQUEST;

import com.orientechnologies.orient.client.remote.message.tx.ORecordOperationRequest;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkDistributed;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedExecutor;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeResponse;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.OTransactionFirstPhaseResult.Type;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.results.OConcurrentModificationResult;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.results.OExceptionResult;
import com.orientechnologies.orient.distributed.impl.coordinator.transaction.results.OUniqueKeyViolationResult;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OTransactionFirstPhaseOperation implements ONodeRequest {

  private OSessionOperationId operationId;
  private List<ORecordOperationRequest> operations;
  private List<OIndexOperationRequest> indexes;

  public OTransactionFirstPhaseOperation(
      OSessionOperationId operationId,
      List<ORecordOperationRequest> operations,
      List<OIndexOperationRequest> indexes) {
    this.operationId = operationId;
    this.operations = operations;
    this.indexes = indexes;
  }

  public OTransactionFirstPhaseOperation() {}

  @Override
  public ONodeResponse execute(
      ONodeIdentity nodeFrom,
      OLogId opId,
      ODistributedExecutor executor,
      ODatabaseDocumentInternal session) {
    ONodeResponse response;
    try {
      ((ODatabaseDocumentDistributed) session).txFirstPhase(operationId, operations, indexes);
      response = new OTransactionFirstPhaseResult(Type.SUCCESS, null);

    } catch (OConcurrentModificationException ex) {
      OConcurrentModificationResult metadata =
          new OConcurrentModificationResult(
              (ORecordId) ex.getRid().getIdentity(),
              ex.getEnhancedRecordVersion(),
              ex.getEnhancedDatabaseVersion());
      response = new OTransactionFirstPhaseResult(Type.CONCURRENT_MODIFICATION_EXCEPTION, metadata);
    } catch (ORecordDuplicatedException ex) {
      OUniqueKeyViolationResult metadata =
          new OUniqueKeyViolationResult(
              ex.getKey().toString(),
              null,
              (ORecordId) ex.getRid().getIdentity(),
              ex.getIndexName());
      response = new OTransactionFirstPhaseResult(Type.UNIQUE_KEY_VIOLATION, metadata);
    } catch (RuntimeException ex) {
      // TODO: get action with some exception handler to offline the node or activate a recover
      // operation
      response = new OTransactionFirstPhaseResult(Type.EXCEPTION, new OExceptionResult(ex));
    }
    return response;
  }

  public static List<ORecordOperation> convert(
      ODatabaseDocumentInternal database, List<ORecordOperationRequest> operations) {
    List<ORecordOperation> ops = new ArrayList<>();
    for (ORecordOperationRequest req : operations) {
      byte type = req.getType();
      if (type == ORecordOperation.LOADED) {
        continue;
      }

      ORecord record = null;
      switch (type) {
        case ORecordOperation.CREATED:
          record = ORecordSerializerNetworkDistributed.INSTANCE.fromStream(req.getRecord(), null);
          ORecordInternal.setRecordSerializer(record, database.getSerializer());
          break;
        case ORecordOperation.UPDATED:
          {
            OIdentifiable updateRecord;
            record = ORecordSerializerNetworkDistributed.INSTANCE.fromStream(req.getRecord(), null);
            ORecordInternal.setRecordSerializer(record, database.getSerializer());
          }
          break;
        case ORecordOperation.DELETED:
          record = database.getRecord(req.getId());
          if (record == null) {
            record =
                Orient.instance()
                    .getRecordFactoryManager()
                    .newInstance(req.getRecordType(), req.getId().getClusterId(), database);
          }
          break;
      }
      if (type == ORecordOperation.CREATED
          || type == ORecordOperation.DELETED
          || (type == ORecordOperation.UPDATED)) {
        ORecordInternal.setIdentity(record, (ORecordId) req.getId());
        ORecordInternal.setVersion(record, req.getVersion());
        ORecordOperation op = new ORecordOperation(record, type);
        ops.add(op);
      }
    }
    return ops;
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    operationId.serialize(output);
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
  public void deserialize(DataInput input) throws IOException {
    operationId = new OSessionOperationId();
    operationId.deserialize(input);

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
  public int getRequestType() {
    return TRANSACTION_FIRST_PHASE_REQUEST;
  }

  public List<OIndexOperationRequest> getIndexes() {
    return indexes;
  }

  public List<ORecordOperationRequest> getOperations() {
    return operations;
  }

  public OSessionOperationId getOperationId() {
    return operationId;
  }
}
