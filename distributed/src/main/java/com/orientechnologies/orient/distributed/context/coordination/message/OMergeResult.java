package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OMergeResult implements OStructuralMessage {

  private ONodeId node;
  private OTransactionIdPromise promise;
  private boolean accepted;

  public OMergeResult(ONodeId node, OTransactionIdPromise promise, boolean accepted) {
    this.node = node;
    this.promise = promise;
    this.accepted = accepted;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    ctx.confirmMerge(this.node, this.promise, this.accepted);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    this.node.writeNetwork(out);
    this.promise.writeNetwork(out);
    out.writeBoolean(this.accepted);
  }

  @Override
  public short getType() {
    return 13;
  }

  public static OMergeResult fromNetwork(DataInput input) throws IOException {
    ONodeId nodeId = ONodeId.readNetwork(input);
    OTransactionIdPromise promise = OTransactionIdPromise.readNetwork(input);
    boolean accepted = input.readBoolean();
    return new OMergeResult(nodeId, promise, accepted);
  }

  public ONodeId getNode() {
    return node;
  }

  public OTransactionIdPromise getPromise() {
    return promise;
  }

  public boolean isAccepted() {
    return accepted;
  }
}
