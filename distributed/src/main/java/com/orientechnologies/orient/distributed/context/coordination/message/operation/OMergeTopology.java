package com.orientechnologies.orient.distributed.context.coordination.message.operation;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.OVersion;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class OMergeTopology implements OOperationMessage {

  private final ONodeId node;
  private final ONodeStateNetwork state;
  private final OVersion version;

  public OMergeTopology(ONodeId node, ONodeStateNetwork state, OVersion version) {
    this.node = node;
    this.state = state;
    this.version = version;
  }

  @Override
  public Optional<OAcceptResult> validate(OrientDBDistributed ctx, OTransactionIdPromise promise) {
    return ctx.getNodeState()
        .getOps()
        .validateMergeNode(this.node, this.state, this.version, promise);
  }

  @Override
  public void apply(OrientDBDistributed ctx, OTransactionIdPromise promise) {
    ctx.mergeNode(this.node, this.state, this.version, promise);
  }

  @Override
  public void cancel(OrientDBDistributed ctx, OTransactionIdPromise promise) {
    ctx.getNodeState().getOps().cancelMergeNode(this.node, this.state, this.version, promise);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    this.node.writeNetwork(out);
    this.state.writeNetwork(out);
    this.version.writeNetwork(out);
  }

  @Override
  public short getType() {
    return 9;
  }

  public static OMergeTopology readNetwork(DataInput input) throws IOException {
    var node = ONodeId.readNetwork(input);
    var state = ONodeStateNetwork.fromNetwork(input);
    var version = OVersion.readNetwork(input);
    return new OMergeTopology(node, state, version);
  }

  @Override
  public String toString() {
    return "OMergeTopology [node=" + node + ", state=" + state + ", version=" + version + "]";
  }
}
