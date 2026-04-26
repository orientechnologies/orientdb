package com.orientechnologies.orient.distributed.context.coordination.message.operation;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class OMergeNode implements OOperationMessage {

  private final ONodeId node;
  private final ONodeStateNetwork state;
  private final ONodeStateNetwork original;

  public OMergeNode(ONodeId node, ONodeStateNetwork state, ONodeStateNetwork original) {
    this.node = node;
    this.state = state;
    this.original = original;
  }

  @Override
  public Optional<OAcceptResult> validate(OrientDBDistributed ctx, OTransactionIdPromise promise) {
    return ctx.getNodeState()
        .getOps()
        .validateMergeNode(this.node, this.state, this.original, promise);
  }

  @Override
  public void apply(OrientDBDistributed ctx, OTransactionIdPromise promise) {
    ctx.mergeNode(this.node, this.state, this.original, promise);
  }

  @Override
  public void cancel(OrientDBDistributed ctx, OTransactionIdPromise promise) {
    ctx.getNodeState().getOps().cancelMergeNode(this.node, this.state, this.original, promise);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    this.node.writeNetwork(out);
    this.state.writeNetwork(out);
    this.original.writeNetwork(out);
  }

  @Override
  public short getType() {
    return 9;
  }

  public static OMergeNode readNetwork(DataInput input) throws IOException {
    var node = ONodeId.readNetwork(input);
    var state = ONodeStateNetwork.fromNetwork(input);
    var original = ONodeStateNetwork.fromNetwork(input);
    return new OMergeNode(node, state, original);
  }

  @Override
  public String toString() {
    return "OMergeNode [node=" + node + ", state=" + state + ", original=" + original + "]";
  }
}
