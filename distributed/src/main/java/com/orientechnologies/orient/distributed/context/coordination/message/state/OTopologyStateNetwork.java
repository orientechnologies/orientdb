package com.orientechnologies.orient.distributed.context.coordination.message.state;

import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.topology.OTopologyState;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class OTopologyStateNetwork {
  private final int quorum;
  private final long version;
  private final OTopologyState state;
  private final Set<ONodeId> members;
  private OGroupId groupId;
  private boolean merge;

  public static OTopologyStateNetwork boot(OGroupId groupId) {
    return new OTopologyStateNetwork(groupId, OTopologyState.BOOT, Collections.emptySet(), 0, 0);
  }

  public OTopologyStateNetwork(
      OGroupId groupId, OTopologyState state, Set<ONodeId> members, int quorum, long version) {
    super();
    assert (state == OTopologyState.BOOT && members.isEmpty() && version == 0)
        || (state == OTopologyState.ESTABLISHED);
    this.groupId = groupId;
    this.state = state;
    this.members = members;
    this.version = version;
    this.quorum = quorum;
  }

  public void writeNetwork(DataOutput output) throws IOException {
    switch (state) {
      case BOOT -> {
        output.writeByte(1);
        this.groupId.writeNetwork(output);
      }
      case ESTABLISHED -> {
        output.writeByte(2);
        this.groupId.writeNetwork(output);
        output.writeLong(version);
        output.writeInt(quorum);
        output.writeBoolean(merge);
        output.writeInt(members.size());
        for (ONodeId node : members) {
          node.writeNetwork(output);
        }
      }
    }
  }

  public static OTopologyStateNetwork fromNetwork(DataInput input) throws IOException {
    byte state = input.readByte();
    switch (state) {
      case 1:
        {
          OGroupId networkId = OGroupId.readNetwork(input);
          return new OTopologyStateNetwork(networkId, OTopologyState.BOOT, new HashSet<>(), 0, 0);
        }
      case 2:
        {
          OGroupId networkId = OGroupId.readNetwork(input);
          long version = input.readLong();
          int quorum = input.readInt();
          boolean merge = input.readBoolean();
          int size = input.readInt();
          Set<ONodeId> members = new HashSet<ONodeId>(size);
          while (size-- > 0) {
            ONodeId node = ONodeId.readNetwork(input);
            members.add(node);
          }
          var topology =
              new OTopologyStateNetwork(
                  networkId, OTopologyState.ESTABLISHED, members, quorum, version);
          topology.setMerge(merge);
          return topology;
        }
      default:
        {
          throw new IOException("found wrong topology id in the network");
        }
    }
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

  public OGroupId getGroupId() {
    return groupId;
  }

  public int getQuorum() {
    return quorum;
  }

  public void setMerge(boolean merge) {
    this.merge = merge;
  }

  public boolean isMerge() {
    return merge;
  }
}
