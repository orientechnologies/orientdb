package com.orientechnologies.orient.distributed.network;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.impl.log.OLogId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class OBroadcastMessage {

  public static class DatabasePing {
    public DatabasePing() {}

    public DatabasePing(String databaseName, OLogId logId) {
      this.databaseName = databaseName;
      this.logId = logId;
    }

    protected String databaseName;
    protected OLogId logId;
  }

  protected static final int TYPE_PING = 0;
  protected static final int TYPE_LEAVE = 1;
  protected static final int TYPE_KNOWN_SERVERS = 2;

  protected static final int TYPE_START_LEADER_ELECTION = 3;
  protected static final int TYPE_VOTE_LEADER_ELECTION = 4;
  protected static final int TYPE_LEADER_ELECTED = 5;

  protected static final int ROLE_COORDINATOR = 0;
  protected static final int ROLE_REPLICA = 1;
  protected static final int ROLE_UNDEFINED = 2;

  protected int type;
  protected ONodeIdentity nodeIdentity;
  protected String group;
  protected int term;
  protected int role;
  protected String connectionUsername;
  protected String connectionPassword;

  // for ping
  protected int tcpPort;

  // for leader election
  protected ONodeIdentity voteForIdentity;
  protected String dbName;
  protected long lastLogId;

  // MASTER INFO

  protected ONodeIdentity leaderIdentity;
  protected int leaderTerm;
  protected String leaderAddress;
  protected int leaderTcpPort;
  protected String leaderConnectionUsername;
  protected String leaderConnectionPassword;
  protected long leaderPing;

  protected List<DatabasePing> databasePings;

  public void write(DataOutput output) throws IOException {
    output.writeInt(type);
    output.writeUTF(group);
    nodeIdentity.serialize(output);
    output.writeInt(term);
    output.writeInt(role);
    output.writeInt(tcpPort);
    output.writeLong(lastLogId);
    output.writeUTF(connectionUsername);
    output.writeUTF(connectionPassword);

    switch (type) {
      case OBroadcastMessage.TYPE_PING:
        if (this.leaderIdentity != null) {
          output.writeByte(1);
          leaderIdentity.serialize(output);
          output.writeInt(leaderTerm);
          output.writeUTF(leaderAddress);
          output.writeInt(leaderTcpPort);
          output.writeLong(leaderPing);
          output.writeUTF(leaderConnectionUsername);
          output.writeUTF(leaderConnectionPassword);
        } else {
          output.writeByte(0);
        }
        break;
      case OBroadcastMessage.TYPE_LEADER_ELECTED:
        output.writeUTF(connectionPassword);
        output.writeUTF(connectionUsername);
        break;
      case OBroadcastMessage.TYPE_VOTE_LEADER_ELECTION:
        voteForIdentity.serialize(output);
        break;
    }

    // database pings
    if (databasePings == null) {
      output.writeInt(0);
    } else {
      output.writeInt(databasePings.size());
      for (DatabasePing databasePing : databasePings) {
        output.writeUTF(databasePing.databaseName);
        OLogId.serialize(databasePing.logId, output);
      }
    }
  }

  public void read(DataInput input) throws IOException {
    type = input.readInt();
    group = input.readUTF();
    nodeIdentity = new ONodeIdentity();
    nodeIdentity.deserialize(input);
    term = input.readInt();
    role = input.readInt();
    tcpPort = input.readInt();
    lastLogId = input.readLong();
    connectionUsername = input.readUTF();
    connectionPassword = input.readUTF();
    switch (type) {
      case OBroadcastMessage.TYPE_PING:
        byte isThereMaster = input.readByte();
        if (isThereMaster == 1) {
          leaderIdentity = new ONodeIdentity();
          leaderIdentity.deserialize(input);
          leaderTerm = input.readInt();
          leaderAddress = input.readUTF();
          leaderTcpPort = input.readInt();
          leaderPing = input.readLong();
          leaderConnectionUsername = input.readUTF();
          leaderConnectionPassword = input.readUTF();
        }
        break;
      case OBroadcastMessage.TYPE_VOTE_LEADER_ELECTION:
        voteForIdentity = new ONodeIdentity();
        voteForIdentity.deserialize(input);
        break;
    }

    // database pings
    int nDbPings = input.readInt();
    databasePings = new ArrayList<>();
    for (int i = 0; i < nDbPings; i++) {
      String nextDbName = input.readUTF();
      OLogId logId = OLogId.deserialize(input);
      databasePings.add(new DatabasePing(nextDbName, logId));
    }
  }

  public ODiscoveryListener.NodeData toNodeData() {
    ODiscoveryListener.NodeData data = new ODiscoveryListener.NodeData();
    data.term = term;
    data.identity = nodeIdentity;
    // TODO: this should be in the message because can be configured
    // data.address = fromAddr;
    data.connectionUsername = connectionUsername;
    data.connectionPassword = connectionPassword;
    data.port = tcpPort;
    return data;
  }

  public ONodeIdentity getNodeIdentity() {
    return nodeIdentity;
  }
}
