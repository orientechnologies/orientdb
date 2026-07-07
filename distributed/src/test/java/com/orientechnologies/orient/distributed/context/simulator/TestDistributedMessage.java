package com.orientechnologies.orient.distributed.context.simulator;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.ODistributedMessage;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationContext;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationMessage;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import java.util.Optional;

public record TestDistributedMessage(OOperationMessage message, OTransactionIdPromise promise)
    implements ODistributedMessage {

  @Override
  public OTransactionIdPromise getPromiseId() {
    return promise;
  }

  @Override
  public Optional<OAcceptResult> validate(OOperationContext ctx) {
    return message.validate(ctx, promise);
  }

  @Override
  public void apply(OOperationContext ctx) {
    message.apply(ctx, promise);
  }

  @Override
  public void cancel(OOperationContext ctx) {
    message.cancel(ctx, promise);
  }

  @Override
  public OOperationMessage getOp() {
    return message;
  }

  @Override
  public void recoordinate(OOperationContext ctx) {
    ctx.recoordinateOperation(promise, message);
  }
}
