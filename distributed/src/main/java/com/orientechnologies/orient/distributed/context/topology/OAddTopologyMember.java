package com.orientechnologies.orient.distributed.context.topology;

import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.result.OAcceptResult;
import com.orientechnologies.orient.distributed.context.coordination.result.OInvalidSequentialAcceptResult;
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
    if (ctx.getNodeState().getTopology().promise(version, node)) {
      return Optional.empty();
    } else {
      return Optional.of(new OInvalidSequentialAcceptResult());
    }
  }

  @Override
  public void apply(OrientDBInternal ctx) {
    ((OrientDBDistributed) ctx).getNodeState().getTopology().confirm(version);
    ((OrientDBDistributed) ctx).getNodeState().register(node);
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
}
