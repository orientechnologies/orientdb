package com.orientechnologies.orient.distributed.impl.coordinator.transaction;

import static com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory.TRANSACTION_SECOND_PHASE_REQUEST;

import com.orientechnologies.orient.client.remote.message.tx.ORecordOperationRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.distributed.impl.OTransactionOptimisticDistributed;
import com.orientechnologies.orient.distributed.impl.coordinator.ODistributedExecutor;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeRequest;
import com.orientechnologies.orient.distributed.impl.coordinator.ONodeResponse;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class OTransactionSecondPhaseOperation implements ONodeRequest {
  private OSessionOperationId operationId;
  private List<ORecordOperationRequest> operations;
  private List<OIndexOperationRequest> indexes;
  private boolean success;

  public OTransactionSecondPhaseOperation(
      OSessionOperationId operationId,
      List<ORecordOperationRequest> operations,
      List<OIndexOperationRequest> indexes,
      boolean success) {
    this.operationId = operationId;
    this.operations = operations;
    this.indexes = indexes;
    this.success = success;
  }

  public OTransactionSecondPhaseOperation() {}

  @Override
  public ONodeResponse execute(
      ONodeIdentity nodeFrom,
      OLogId opId,
      ODistributedExecutor executor,
      ODatabaseDocumentInternal session) {
    OTransactionOptimisticDistributed tx =
        ((ODatabaseDocumentDistributed) session)
            .txSecondPhase(operationId, operations, indexes, success);

    List<OCreatedRecordResponse> createdRecords = new ArrayList<>(tx.getCreatedRecords().size());
    List<OUpdatedRecordResponse> updatedRecords = new ArrayList<>(tx.getUpdatedRecords().size());
    List<ODeletedRecordResponse> deletedRecords = new ArrayList<>(tx.getDeletedRecord().size());
    if (tx != null) {
      for (Map.Entry<ORID, ORecord> entry : tx.getCreatedRecords().entrySet()) {
        ORecord record = entry.getValue();
        createdRecords.add(
            new OCreatedRecordResponse(entry.getKey(), record.getIdentity(), record.getVersion()));
      }

      for (Map.Entry<ORID, ORecord> entry : tx.getUpdatedRecords().entrySet()) {
        updatedRecords.add(
            new OUpdatedRecordResponse(entry.getKey(), entry.getValue().getVersion()));
      }

      for (ORID id : tx.getDeletedRecord()) {
        deletedRecords.add(new ODeletedRecordResponse(id));
      }
    }

    return new OTransactionSecondPhaseResponse(
        true, createdRecords, updatedRecords, deletedRecords);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OTransactionSecondPhaseOperation that = (OTransactionSecondPhaseOperation) o;
    return success == that.success;
  }

  @Override
  public int hashCode() {
    return Objects.hash(success);
  }

  @Override
  public void serialize(DataOutput output) throws IOException {
    operationId.serialize(output);
    output.writeBoolean(success);
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
    success = input.readBoolean();

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
    return TRANSACTION_SECOND_PHASE_REQUEST;
  }

  public OSessionOperationId getOperationId() {
    return operationId;
  }

  public boolean isSuccess() {
    return success;
  }
}
