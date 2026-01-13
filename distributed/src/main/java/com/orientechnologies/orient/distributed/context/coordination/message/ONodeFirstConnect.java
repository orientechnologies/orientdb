package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.message.state.ONodeStateNetwork;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ONodeFirstConnect implements OStructuralMessage {

  private final ONodeId nodeId;
  private final ONodeStateNetwork state;

  public ONodeFirstConnect(ONodeId nodeId, ONodeStateNetwork state) {
    this.nodeId = nodeId;
    this.state = state;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    ctx.firstConnect(nodeId, state);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    nodeId.writeNetwork(out);
    state.writeNetwork(out);
  }

  public static ONodeFirstConnect fromNetwork(DataInput input) throws IOException {
    ONodeId nodeId = ONodeId.readNetwork(input);
    ONodeStateNetwork state = ONodeStateNetwork.fromNetwork(input);
    return new ONodeFirstConnect(nodeId, state);
  }

  @Override
  public short getType() {
    return 6;
  }

  public ONodeId getNodeId() {
    return nodeId;
  }

  public ONodeStateNetwork getState() {
    return state;
  }
}
