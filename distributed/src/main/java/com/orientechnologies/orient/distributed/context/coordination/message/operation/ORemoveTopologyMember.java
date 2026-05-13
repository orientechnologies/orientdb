package com.orientechnologies.orient.distributed.context.coordination.message.operation;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.OVersion;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class ORemoveTopologyMember implements OOperationMessage {

  private ONodeId node;
  private OVersion version;

  public ORemoveTopologyMember(ONodeId node, OVersion version) {
    this.node = node;
    this.version = version;
  }

  @Override
  public Optional<OAcceptResult> validate(OOperationContext ctx, OTransactionIdPromise promise) {
    return ctx.getNodeState().getOps().validateUnregisterNode(node, version, promise);
  }

  @Override
  public void apply(OOperationContext ctx, OTransactionIdPromise promise) {
    ctx.getNodeState().getOps().unregisterNode(node, version, promise);
  }

  @Override
  public void cancel(OOperationContext ctx, OTransactionIdPromise promise) {
    ctx.getNodeState().getOps().cancelUnregisterNode(promise);
  }

  @Override
  public void serialize(DataOutput out) throws IOException {
    this.node.writeNetwork(out);
    this.version.writeNetwork(out);
  }

  @Override
  public short getType() {
    return 8;
  }

  public static ORemoveTopologyMember readNetwork(DataInput input) throws IOException {
    ONodeId node = ONodeId.readNetwork(input);
    OVersion version = OVersion.readNetwork(input);
    return new ORemoveTopologyMember(node, version);
  }

  @Override
  public String toString() {
    return "ORemoveTopologyMember [node=" + node + ", version=" + version + "]";
  }

  public ONodeId getNode() {
    return node;
  }

  public OVersion getVersion() {
    return version;
  }
}
