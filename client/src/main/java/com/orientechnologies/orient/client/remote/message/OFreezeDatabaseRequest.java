package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public class OFreezeDatabaseRequest implements OBinaryRequest {
  private String name;
  private String type;

  public OFreezeDatabaseRequest(String name, String type) {
    super();
    this.name = name;
    this.type = type;
  }

  public OFreezeDatabaseRequest() {
    // TODO Auto-generated constructor stub
  }

  @Override
  public void write(OChannelBinaryAsynchClient network, OStorageRemoteSession session, int mode) throws IOException {
    network.writeString(name);
    network.writeString(type);

  }

  @Override
  public void read(OChannelBinary channel, int protocolVersion, String serializerName) throws IOException {
    name = channel.readString();
    type = channel.readString();

  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_DB_FREEZE;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

}