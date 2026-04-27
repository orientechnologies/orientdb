package com.orientechnologies.orient.distributed.context.coordination.message.state;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import com.orientechnologies.orient.distributed.context.coordination.OVersion;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public record ODatabaseStateNetwork(
    ODatabaseId id,
    String name,
    int quorum,
    OVersion version,
    List<ODatabaseMemberNetwork> members) {

  public void writeNetwork(DataOutput output) throws IOException {
    this.id.writeNetwork(output);
    output.writeUTF(name);
    output.writeInt(quorum);
    version.writeNetwork(output);
    output.writeInt(members.size());
    for (ODatabaseMemberNetwork member : this.members) {
      member.writeNetwork(output);
    }
  }

  public static ODatabaseStateNetwork readNetwork(DataInput input) throws IOException {
    var id = ODatabaseId.readNetwork(input);
    var name = input.readUTF();
    var quorum = input.readInt();
    var version = OVersion.readNetwork(input);
    int membersSize = input.readInt();
    List<ODatabaseMemberNetwork> members = new ArrayList<>();
    while (membersSize-- > 0) {
      members.add(ODatabaseMemberNetwork.readNetwork(input));
    }
    return new ODatabaseStateNetwork(id, name, quorum, version, members);
  }
}
