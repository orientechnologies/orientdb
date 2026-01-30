package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.ONodeState;
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
    ONodeState nodeState = ctx.getNodeState();
    ONodeId coordinator = promise.getCoordinator();
    Optional<OAcceptResult> result = nodeState.receive(this);
    if (result.isEmpty()) {
      Optional<OAcceptResult> res = op.validate(ctx, promise);
      if (res.isPresent()) {
        // This is canceling the promise right away because is not accepted by the data
        nodeState.cancelPromise(promise);
        var failure = new OFailPropose(nodeState.getNodeId(), promise, res.get());
        ctx.sendMessage(coordinator, failure);
      } else {
        var success = new OSuccessPropose(nodeState.getNodeId(), promise);
        ctx.sendMessage(coordinator, success);
      }
    } else {
      var failure = new OFailPropose(nodeState.getNodeId(), promise, result.get());
      ctx.sendMessage(coordinator, failure);
    }
  }

  @Override
  public void apply(OrientDBDistributed ctx) {
    this.op.apply(ctx, promise);
  }

  @Override
  public void cancel(OrientDBDistributed ctx) {
    this.op.cancel(ctx, promise);
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
