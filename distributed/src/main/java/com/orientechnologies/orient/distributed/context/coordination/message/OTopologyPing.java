package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OTopologyPing implements OStructuralMessage {

  private final ONodeId nodeId;
  private final OTransactionSequenceStatus status;

  public OTopologyPing(ONodeId nodeId, OTransactionSequenceStatus status) {
    super();
    this.nodeId = nodeId;
    this.status = status;
  }

  @Override
  public void execute(OrientDBDistributed ctx) {
    ctx.receivePing(nodeId, status);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    this.nodeId.writeNetwork(out);
    this.status.writeNetwork(out);
  }

  @Override
  public short getType() {
    return 17;
  }

  public static OTopologyPing fromNetwork(DataInput input) throws IOException {
    var node = ONodeId.readNetwork(input);
    var status = OTransactionSequenceStatus.readNetwork(input);
    return new OTopologyPing(node, status);
  }

  public ONodeId getNodeId() {
    return nodeId;
  }

  public OTransactionSequenceStatus getStatus() {
    return status;
  }

  @Override
  public String toString() {
    return "OTopologyPing [nodeId=" + nodeId + ", status=" + status + "]";
  }
}
