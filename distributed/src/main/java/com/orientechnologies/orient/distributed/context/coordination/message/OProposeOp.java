package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.ONodeState;
import com.orientechnologies.orient.distributed.db.OOperationMessage;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import com.orientechnologies.orient.server.distributed.ODistributedMessage;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
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
    boolean result = nodeState.receive(this);
    if (result) {
      Optional<OAcceptResult> res = op.validate(ctx);
      if (res.isPresent()) {
        ctx.sendMessage(
            Collections.singleton(promise.getCoordinator()),
            new OFailPropose(nodeState.getNodeId(), promise, res.get()));
      } else {
        ctx.sendMessage(
            Collections.singleton(promise.getCoordinator()),
            new OSuccessPropose(nodeState.getNodeId(), promise));
      }
    } else {
      ctx.sendMessage(
          Collections.singleton(promise.getCoordinator()),
          new OFailPropose(nodeState.getNodeId(), promise, new OInvalidSequentialAcceptResutl()));
    }
  }

  @Override
  public void apply(OrientDBInternal ctx) {
    this.op.apply(ctx);
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
}
