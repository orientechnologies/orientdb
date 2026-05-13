package com.orientechnologies.orient.distributed.context.coordination.message.operation;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.OVersion;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class OAddTopologyMember implements OOperationMessage {

  private final OVersion version;
  private final ONodeId node;

  public OAddTopologyMember(OVersion version, ONodeId node) {
    this.version = version;
    this.node = node;
  }

  @Override
  public Optional<OAcceptResult> validate(OOperationContext ctx, OTransactionIdPromise promise) {
    return ctx.getNodeState().getOps().validateRegisterNode(node, version, promise);
  }

  @Override
  public void apply(OOperationContext ctx, OTransactionIdPromise promise) {
    ctx.registerNode(node, version, promise);
  }

  @Override
  public void cancel(OOperationContext ctx, OTransactionIdPromise promise) {
    ctx.cancelRegisterPromise(promise);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    this.version.writeNetwork(out);
    this.node.writeNetwork(out);
  }

  public static OAddTopologyMember readNetwork(DataInput input) throws IOException {
    var version = OVersion.readNetwork(input);
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

  public OVersion getVersion() {
    return version;
  }

  @Override
  public String toString() {
    return "Add " + node + " to network, version=" + version;
  }
}
