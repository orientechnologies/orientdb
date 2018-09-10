package com.orientechnologies.orient.server.distributed.impl.coordinator.transaction;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.distributed.impl.coordinator.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static com.orientechnologies.orient.server.distributed.impl.coordinator.OCoordinateMessagesFactory.TRANSACTION_SECOND_PHASE_REQUEST;

public class OTransactionSecondPhaseOperation implements ONodeRequest {
  private OSessionOperationId operationId;
  private boolean             success;

  public OTransactionSecondPhaseOperation(OSessionOperationId operationId, boolean success) {
    this.operationId = operationId;
    this.success = success;
  }

  public OTransactionSecondPhaseOperation() {

  }

  @Override
  public ONodeResponse execute(ODistributedMember nodeFrom, OLogId opId, ODistributedExecutor executor,
      ODatabaseDocumentInternal session) {
    ((ODatabaseDocumentDistributed) session).txSecondPhase(operationId, success);
    return new OTransactionSecondPhaseResponse(true);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
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
  }

  @Override
  public void deserialize(DataInput input) throws IOException {
    operationId = new OSessionOperationId();
    operationId.deserialize(input);
    success = input.readBoolean();
  }

  @Override
  public int getRequestType() {
    return TRANSACTION_SECOND_PHASE_REQUEST;
  }
}
