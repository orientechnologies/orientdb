package com.orientechnologies.orient.distributed.context.topology;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.OInvalidSequential;
import com.orientechnologies.orient.distributed.db.OOperationMessage;
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
  public Optional<OAcceptResult> validate(OrientDBDistributed ctx) {
    if (ctx.getNodeState().promiseRegister(node, version)) {
      return Optional.empty();
    } else {
      return Optional.of(new OInvalidSequential());
    }
  }

  @Override
  public void apply(OrientDBDistributed ctx) {
    ctx.getNodeState().register(node, version);
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
}
