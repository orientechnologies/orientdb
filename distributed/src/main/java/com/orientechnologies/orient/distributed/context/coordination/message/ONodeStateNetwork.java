package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.topology.OTopologyState;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ONodeStateNetwork {
  private final long version;
  private final OTopologyState state;
  private final Set<ONodeId> members;

  public ONodeStateNetwork(OTopologyState state, Set<ONodeId> members, long version) {
    super();
    this.state = state;
    this.members = members;
    this.version = version;
  }

  public void writeNetwork(DataOutput output) throws IOException {
    output.writeLong(version);
    switch (state) {
      case BOOT -> output.writeByte(1);
      case ESTABLISHED -> output.writeByte(2);
    }

    output.writeInt(members.size());
    for (ONodeId node : members) {
      node.writeNetwork(output);
    }
  }

  public static ONodeStateNetwork fromNetwork(DataInput input) throws IOException {
    long version = input.readLong();
    byte state = input.readByte();
    OTopologyState s;
    switch (state) {
      case 1:
        {
          s = OTopologyState.BOOT;
          break;
        }
      case 2:
        {
          s = OTopologyState.ESTABLISHED;
          break;
        }
      default:
        {
          throw new IOException("found wrong topology id in the network");
        }
    }
    int size = input.readInt();
    Set<ONodeId> members = new HashSet<ONodeId>(size);
    while (size-- > 0) {
      ONodeId node = ONodeId.readNetwork(input);
      members.add(node);
    }
    return new ONodeStateNetwork(s, members, version);
  }

  public Set<ONodeId> getMembers() {
    return members;
  }

  public OTopologyState getState() {
    return state;
  }

  public long getVersion() {
    return version;
  }
}
