package com.orientechnologies.orient.distributed.context.retryable;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.distributed.context.coordination.OVersion;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.ODropDbMessage;
import com.orientechnologies.orient.distributed.db.OCompleteExecution;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;

public record ODropRetryOperation(ODatabaseId id) implements ORetryOperation {
  @Override
  public void execute(OrientDBDistributed ctx, OCompleteExecution complete) {
    var version = ctx.getNodeState().getOps().nextDatabaseVersion(id);
    ctx.coordinatedOperation(new ODropDbMessage(id, new OVersion(version)), complete);
  }
}
