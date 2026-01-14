package com.orientechnologies.orient.distributed.context.retryable;

import com.orientechnologies.orient.distributed.context.coordination.message.operation.ODropDbMessage;
import com.orientechnologies.orient.distributed.db.OCompleteExecution;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;

public record ODropRetryOperation(String name) implements ORetryOperation {
  @Override
  public void execute(OrientDBDistributed ctx, OCompleteExecution complete) {
    ctx.coordinatedOperation(new ODropDbMessage(name), complete);
  }
}
