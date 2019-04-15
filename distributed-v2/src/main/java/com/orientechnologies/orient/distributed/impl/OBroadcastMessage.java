package com.orientechnologies.orient.distributed.impl;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class OBroadcastMessage {
  static final int TYPE_PING          = 0;
  static final int TYPE_LEAVE         = 1;
  static final int TYPE_KNOWN_SERVERS = 2;

  static final int TYPE_START_LEADER_ELECTION = 3;
  static final int TYPE_VOTE_LEADER_ELECTION  = 4;
  static final int TYPE_LEADER_ELECTED        = 5;

  static final int ROLE_COORDINATOR = 0;
  static final int ROLE_REPLICA     = 1;
  static final int ROLE_UNDEFINED   = 2;

  int           type;
  ONodeIdentity nodeIdentity;
  String        group;
  int           term;
  int           role;
  String        connectionUsername;
  String        connectionPassword;

  //for ping
  int tcpPort;

  // for leader election
  ONodeIdentity voteForIdentity;
  String        dbName;
  long          lastLogId;

  //MASTER INFO

  ONodeIdentity masterIdentity;
  int           masterTerm;
  String        masterAddress;
  int           masterTcpPort;
  String        masterConnectionUsername;
  String        masterConnectionPassword;
  long          masterPing;

  public void write(DataOutput output) throws IOException {
    output.writeInt(type);
    output.writeUTF(group);
    nodeIdentity.serialize(output);
    output.writeInt(term);
    output.writeInt(role);
    output.writeInt(tcpPort);
    output.writeBoolean(connectionUsername != null);
    if (connectionUsername != null) {
      output.writeUTF(connectionUsername);
    }
    output.writeBoolean(connectionPassword != null);
    if (connectionPassword != null) {
      output.writeUTF(connectionPassword);
    }

    switch (type) {
    case OBroadcastMessage.TYPE_PING:
      if (this.masterIdentity != null) {
        output.writeByte(1);
        masterIdentity.serialize(output);
        output.writeInt(masterTerm);
        output.writeUTF(masterAddress);
        output.writeInt(masterTcpPort);
        output.writeLong(masterPing);
        output.writeBoolean(masterConnectionUsername != null);
        if (masterConnectionUsername != null) {
          output.writeUTF(masterConnectionUsername);
        }
        output.writeBoolean(masterConnectionPassword != null);
        if (masterConnectionPassword != null) {
          output.writeUTF(masterConnectionPassword);
        }
      } else {
        output.writeByte(0);
      }
      break;
    case OBroadcastMessage.TYPE_VOTE_LEADER_ELECTION:
      voteForIdentity.serialize(output);
      break;
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
    if (input.readBoolean()) {
      connectionUsername = input.readUTF();
    }
    if (input.readBoolean()) {
      connectionPassword = input.readUTF();
    }

    switch (type) {
    case OBroadcastMessage.TYPE_PING:
      byte isThereMaster = input.readByte();
      if (isThereMaster == 1) {
        masterIdentity = new ONodeIdentity();
        masterIdentity.deserialize(input);
        masterTerm = input.readInt();
        masterAddress = input.readUTF();
        masterTcpPort = input.readInt();
        masterPing = input.readLong();
        if (input.readBoolean()) {
          masterConnectionUsername = input.readUTF();
        }
        if (input.readBoolean()) {
          masterConnectionPassword = input.readUTF();
        }
      }
      break;
    case OBroadcastMessage.TYPE_VOTE_LEADER_ELECTION:
      voteForIdentity = new ONodeIdentity();
      voteForIdentity.deserialize(input);
      break;
    }
  }

  public ODiscoveryListener.NodeData toNodeData() {
    ODiscoveryListener.NodeData data = new ODiscoveryListener.NodeData();
    data.term = term;
    data.identity = nodeIdentity;
    //TODO: this should be in the message because can be configured
    //data.address = fromAddr;
    data.connectionUsername = connectionUsername;
    data.connectionPassword = connectionPassword;
    data.port = tcpPort;
    return data;
  }

  public ONodeIdentity getNodeIdentity() {
    return nodeIdentity;
  }

}
