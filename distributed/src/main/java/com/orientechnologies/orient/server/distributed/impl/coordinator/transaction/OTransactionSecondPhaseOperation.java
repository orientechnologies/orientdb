package com.orientechnologies.orient.server.distributed.impl.coordinator.transaction;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.server.distributed.impl.coordinator.*;

import java.util.Objects;

public class OTransactionSecondPhaseOperation implements ONodeRequest {
  private boolean success;

  public OTransactionSecondPhaseOperation(boolean success) {
    this.success = success;
  }

  @Override
  public ONodeResponse execute(ODistributedMember nodeFrom, OLogId opId, ODistributedExecutor executor,
      ODatabaseDocumentInternal session) {
    return null;
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
}
