package com.orientechnologies.orient.distributed.context.retryable;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.ODropDbMessage;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.db.OCompleteExecution;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.util.Optional;

public record ODropRetryOperation(ODatabaseId id) implements ORetryOperation {
  @Override
  public void execute(
      OrientDBDistributed ctx,
      OCompleteExecution complete,
      Optional<OAcceptResult> previousResult) {
    var version = ctx.getNodeState().getOps().nextDatabaseVersion(id);
    ctx.coordinatedOperation(new ODropDbMessage(id, version), complete);
  }
}
