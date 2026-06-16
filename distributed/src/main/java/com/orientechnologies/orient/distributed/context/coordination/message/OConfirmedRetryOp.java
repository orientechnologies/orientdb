package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationContext;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationMessage;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import java.util.Optional;

public record OConfirmedRetryOp(OTransactionIdPromise promiseId, OOperationMessage op)
    implements ODistributedMessage {

  @Override
  public OTransactionIdPromise getPromiseId() {
    return promiseId;
  }

  @Override
  public OOperationMessage getOp() {
    return op;
  }

  @Override
  public Optional<OAcceptResult> validate(OOperationContext ctx) {
    return op.validate(ctx, getPromiseId());
  }

  @Override
  public void apply(OOperationContext ctx) {
    op.apply(ctx, getPromiseId());
  }

  @Override
  public void cancel(OOperationContext ctx) {
    // Should never fail, so should never cancel
  }

  @Override
  public void recoordinate(OOperationContext ctx) {
    // Is already confirmed no need to re-coordinate, ever
  }
}
