package com.orientechnologies.orient.server.distributed.impl.coordinator.transaction;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.distributed.impl.coordinator.*;

import java.util.List;
import java.util.Objects;

public class OTransactionSecondPhaseOperation implements ONodeRequest {
  private OSessionOperationId operationId;
  private boolean             success;
  private List<ORecordId>     allocatedIds;

  public OTransactionSecondPhaseOperation(OSessionOperationId operationId, boolean success, List<ORecordId> allocatedIds) {
    this.operationId = operationId;
    this.success = success;
    this.allocatedIds = allocatedIds;
  }

  @Override
  public ONodeResponse execute(ODistributedMember nodeFrom, OLogId opId, ODistributedExecutor executor,
      ODatabaseDocumentInternal session) {
    ((ODatabaseDocumentDistributed) session).txSecondPhase(operationId, success);
    return new OTransactionSecondPhaseResponse(true);
  }

  public List<ORecordId> getAllocatedIds() {
    return allocatedIds;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    OTransactionSecondPhaseOperation that = (OTransactionSecondPhaseOperation) o;
    return success == that.success && Objects.equals(allocatedIds, that.allocatedIds);
  }

  @Override
  public int hashCode() {

    return Objects.hash(success, allocatedIds);
  }
}
