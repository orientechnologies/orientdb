package com.orientechnologies.orient.distributed.context.coordination.message.state;

import com.orientechnologies.orient.core.transaction.OGroupId;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.distributed.context.coordination.OVersion;
import com.orientechnologies.orient.distributed.context.coordination.topology.OTopologyState;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public record OTopologyStateNetwork(
    OGroupId groupId, OTopologyState state, Set<ONodeId> members, int quorum, OVersion version) {

  public OTopologyStateNetwork(
      OGroupId groupId, OTopologyState state, Set<ONodeId> members, int quorum, OVersion version) {
    assert (state == OTopologyState.BOOT && members.isEmpty() && version.getValue() == 0)
        || (state == OTopologyState.ESTABLISHED);
    this.groupId = groupId;
    this.state = state;
    this.members = members;
    this.version = version;
    this.quorum = quorum;
  }

  public static OTopologyStateNetwork boot(OGroupId groupId) {
    return new OTopologyStateNetwork(
        groupId, OTopologyState.BOOT, Collections.emptySet(), 0, new OVersion(0));
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
        version.writeNetwork(output);
        output.writeInt(quorum);
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
          return new OTopologyStateNetwork(
              networkId, OTopologyState.BOOT, new HashSet<>(), 0, new OVersion(0));
        }
      case 2:
        {
          OGroupId networkId = OGroupId.readNetwork(input);
          var version = OVersion.readNetwork(input);
          int quorum = input.readInt();
          int size = input.readInt();
          Set<ONodeId> members = new HashSet<ONodeId>(size);
          while (size-- > 0) {
            ONodeId node = ONodeId.readNetwork(input);
            members.add(node);
          }
          var topology =
              new OTopologyStateNetwork(
                  networkId, OTopologyState.ESTABLISHED, members, quorum, version);
          return topology;
        }
      default:
        {
          throw new IOException("found wrong topology id in the network");
        }
    }
  }
}
