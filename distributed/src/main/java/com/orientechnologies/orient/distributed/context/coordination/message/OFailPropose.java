package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OFailPropose implements OStructuralMessage {

  private ONodeId nodeId;
  private OTransactionIdPromise promise;
  private OAcceptResult acceptResult;

  public OFailPropose(ONodeId nodeId, OTransactionIdPromise promise, OAcceptResult acceptResult) {
    // TOOD: handle failure reason
    this.nodeId = nodeId;
    this.promise = promise;
    this.acceptResult = acceptResult;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    // TODO: pass a failure reason
    ctx.getNodeState().failure(nodeId, promise);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    nodeId.writeNetwork(out);
    promise.writeNetwork(out);
    acceptResult.writeNetwork(out);
  }

  public static OFailPropose fromNetwork(DataInput input) throws IOException {
    ONodeId nodeId = ONodeId.readNetwork(input);
    OTransactionIdPromise promise = OTransactionIdPromise.readNetwork(input);
    OAcceptResult acceptResult = OAcceptResult.readNetwork(input);
    return new OFailPropose(nodeId, promise, acceptResult);
  }

  @Override
  public short getType() {
    return 3;
  }

  public ONodeId getNodeId() {
    return nodeId;
  }

  public OTransactionIdPromise getPromise() {
    return promise;
  }

  public OAcceptResult getAcceptResult() {
    return acceptResult;
  }
}
