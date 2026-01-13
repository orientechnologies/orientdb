package com.orientechnologies.orient.distributed.context.coordination.message.operation;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class OAddTopologyMember implements OOperationMessage {

  private final long version;
  private final ONodeId node;

  public OAddTopologyMember(long version, ONodeId node) {
    this.version = version;
    this.node = node;
  }

  @Override
  public Optional<OAcceptResult> validate(OrientDBDistributed ctx, OTransactionIdPromise promise) {
    return ctx.getNodeState().getOps().validateRegisterNode(node, version);
  }

  @Override
  public void apply(OrientDBDistributed ctx, OTransactionIdPromise promise) {
    ctx.registerNode(node, version);
  }

  @Override
  public void cancel(OrientDBDistributed ctx, OTransactionIdPromise promise) {
    ctx.cancelRegisterPromise();
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    out.writeLong(version);
    this.node.writeNetwork(out);
  }

  public static OAddTopologyMember readNetwork(DataInput input) throws IOException {
    long version = input.readLong();
    ONodeId node = ONodeId.readNetwork(input);
    return new OAddTopologyMember(version, node);
  }

  @Override
  public short getType() {
    return 2;
  }

  public ONodeId getNode() {
    return node;
  }

  public long getVersion() {
    return version;
  }

  @Override
  public String toString() {
    return "Add " + node + " to network, version=" + version;
  }
}
