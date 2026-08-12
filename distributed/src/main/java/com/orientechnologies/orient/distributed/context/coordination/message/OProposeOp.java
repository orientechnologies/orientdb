package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationContext;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationMessage;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class OProposeOp implements OStructuralMessage, ODistributedMessage {
  private OTransactionIdPromise promise;
  private OOperationMessage op;

  public OProposeOp(OTransactionIdPromise promise, OOperationMessage op) {
    this.promise = promise;
    this.op = op;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    var result = ctx.propose(this);

    var coordinator = promise.getCoordinator();
    var current = ctx.getNodeId();
    if (result.isEmpty()) {
      var success = new OSuccessPropose(current, promise);
      ctx.sendMessage(coordinator, success);
    } else {
      var failure = new OFailPropose(current, promise, result.get());
      ctx.sendMessage(coordinator, failure);
    }
  }

  @Override
  public void apply(OOperationContext ctx) {
    this.op.apply(ctx, promise);
  }

  @Override
  public void cancel(OOperationContext ctx) {
    this.op.cancel(ctx, promise);
  }

  @Override
  public void recoordinate(OOperationContext ctx) {
    ctx.recoordinateOperation(promise, op);
  }

  @Override
  public Optional<OAcceptResult> validate(OOperationContext ctx) {
    return op.validate(ctx, promise);
  }

  @Override
  public OTransactionIdPromise getPromiseId() {
    return promise;
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    this.promise.writeNetwork(out);
    this.op.writeNetwork(out);
  }

  public static OProposeOp fromNetwork(DataInput input) throws IOException {
    OTransactionIdPromise promise = OTransactionIdPromise.readNetwork(input);
    OOperationMessage op = OOperationMessage.readNetwork(input);
    return new OProposeOp(promise, op);
  }

  public OOperationMessage getOp() {
    return op;
  }

  @Override
  public short getType() {
    return 1;
  }

  @Override
  public String toString() {
    return "OProposeOp [promise=" + promise + ", op=" + op + "]";
  }
}
