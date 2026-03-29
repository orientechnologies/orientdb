package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.operation.OOperationMessage;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

public class ORetryProposeOp implements OStructuralMessage {

  private OTransactionIdPromise promise;
  private OOperationMessage op;

  public ORetryProposeOp(OTransactionIdPromise promise, OOperationMessage op) {
    this.promise = promise;
    this.op = op;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    var nodeState = ctx.getNodeState();

    Optional<OAcceptResult> result = nodeState.getOps().receiveRetry(this.promise);
    if (result.isEmpty()) {
      ctx.sendMessage(
          Collections.singleton(promise.getCoordinator()),
          new OSuccessPropose(nodeState.getNodeId(), promise));
    } else {
      ctx.sendMessage(
          Collections.singleton(promise.getCoordinator()),
          new OFailPropose(nodeState.getNodeId(), promise, result.get()));
    }
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    this.promise.writeNetwork(out);
    this.op.writeNetwork(out);
  }

  @Override
  public short getType() {
    return 16;
  }

  public static ORetryProposeOp fromNetwork(DataInput input) throws IOException {
    var promise = OTransactionIdPromise.readNetwork(input);
    var op = OOperationMessage.readNetwork(input);
    return new ORetryProposeOp(promise, op);
  }

  public OOperationMessage getOp() {
    return op;
  }

  public OTransactionIdPromise getPromise() {
    return promise;
  }

  @Override
  public String toString() {
    return "Retry promise " + promise + " for operation " + op + "";
  }
}
