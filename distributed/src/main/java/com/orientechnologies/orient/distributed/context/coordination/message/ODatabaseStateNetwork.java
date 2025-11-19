package com.orientechnologies.orient.distributed.context.coordination.message;

import com.orientechnologies.orient.core.transaction.ODatabaseId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ODatabaseStateNetwork {
  private final ODatabaseId id;
  private final String name;
  private final int quorum;
  private final long version;
  private final List<ODatabaseMemberNetwork> members;

  public ODatabaseStateNetwork(
      ODatabaseId id,
      String name,
      int mimimumQuorum,
      long version,
      List<ODatabaseMemberNetwork> members) {
    super();
    this.id = id;
    this.name = name;
    this.quorum = mimimumQuorum;
    this.version = version;
    this.members = members;
  }

  public void writeNetwork(DataOutput output) throws IOException {
    this.id.writeNetwork(output);
    output.writeUTF(name);
    output.writeInt(quorum);
    output.writeLong(version);
    output.writeInt(members.size());
    for (ODatabaseMemberNetwork member : this.members) {
      member.writeNetwork(output);
    }
  }

  public static ODatabaseStateNetwork readNetwork(DataInput input) throws IOException {
    var id = ODatabaseId.readNetwork(input);
    var name = input.readUTF();
    var quorum = input.readInt();
    var version = input.readLong();
    int membersSize = input.readInt();
    List<ODatabaseMemberNetwork> members = new ArrayList<>();
    while (membersSize-- > 0) {
      members.add(ODatabaseMemberNetwork.readNetwork(input));
    }
    return new ODatabaseStateNetwork(id, name, quorum, version, members);
  }

  public ODatabaseId getId() {
    return id;
  }

  public int getQuorum() {
    return quorum;
  }

  public long getVersion() {
    return version;
  }

  public List<ODatabaseMemberNetwork> getMembers() {
    return members;
  }

  public String getName() {
    return name;
  }
}
