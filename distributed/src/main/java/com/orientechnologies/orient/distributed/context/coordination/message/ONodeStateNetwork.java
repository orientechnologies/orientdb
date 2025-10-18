package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.topology.OTopologyState;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class ONodeStateNetwork {
  private final int quorum;
  private final long version;
  private final OTopologyState state;
  private final Set<ONodeId> members;
  private Optional<OGroupId> groupId;

  public static ONodeStateNetwork boot() {
    return new ONodeStateNetwork(
        Optional.empty(), OTopologyState.BOOT, Collections.emptySet(), 0, 0);
  }

  public ONodeStateNetwork(
      Optional<OGroupId> groupId,
      OTopologyState state,
      Set<ONodeId> members,
      int quorum,
      long version) {
    super();
    assert (state == OTopologyState.BOOT && groupId.isEmpty() && members.isEmpty() && version == 0)
        || (state == OTopologyState.ESTABLISHED && groupId.isPresent());
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
      }
      case ESTABLISHED -> {
        output.writeByte(2);
        this.groupId.get().writeNetwork(output);
        output.writeLong(version);
        output.writeInt(quorum);
        output.writeInt(members.size());
        for (ONodeId node : members) {
          node.writeNetwork(output);
        }
      }
    }
  }

  public static ONodeStateNetwork fromNetwork(DataInput input) throws IOException {
    byte state = input.readByte();
    switch (state) {
      case 1:
        {
          return new ONodeStateNetwork(
              Optional.empty(), OTopologyState.BOOT, new HashSet<>(), 0, 0);
        }
      case 2:
        {
          OGroupId networkId = OGroupId.readNetwork(input);
          long version = input.readLong();
          int quorum = input.readInt();
          int size = input.readInt();
          Set<ONodeId> members = new HashSet<ONodeId>(size);
          while (size-- > 0) {
            ONodeId node = ONodeId.readNetwork(input);
            members.add(node);
          }
          return new ONodeStateNetwork(
              Optional.of(networkId), OTopologyState.ESTABLISHED, members, quorum, version);
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

  public Optional<OGroupId> getGroupId() {
    return groupId;
  }

  public int getQuorum() {
    return quorum;
  }
}
