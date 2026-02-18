package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OSuccessPropose implements OStructuralMessage {

  private ONodeId nodeId;
  private OTransactionIdPromise promise;

  public OSuccessPropose(ONodeId nodeId, OTransactionIdPromise promise) {
    this.nodeId = nodeId;
    this.promise = promise;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    ctx.getNodeState().success(nodeId, promise);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    nodeId.writeNetwork(out);
    promise.writeNetwork(out);
  }

  public static OSuccessPropose fromNetwork(DataInput input) throws IOException {
    ONodeId nodeId = ONodeId.readNetwork(input);
    OTransactionIdPromise promise = OTransactionIdPromise.readNetwork(input);
    return new OSuccessPropose(nodeId, promise);
  }

  @Override
  public short getType() {
    return 2;
  }

  public ONodeId getNodeId() {
    return nodeId;
  }

  public OTransactionIdPromise getPromise() {
    return promise;
  }

  @Override
  public String toString() {
    return "OSuccessPropose [nodeId=" + nodeId + ", promise=" + promise + "]";
  }
}
