package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public class OShutdownRequest implements OBinaryRequest {
  private String rootUser;
  private String rootPassword;

  public OShutdownRequest(String rootUser, String rootPassword) {
    super();
    this.rootUser = rootUser;
    this.rootPassword = rootPassword;
  }

  public OShutdownRequest() {
  }

  @Override
  public void write(OChannelBinaryAsynchClient network, OStorageRemoteSession session, int mode) throws IOException {
    network.writeString(rootUser);
    network.writeString(rootPassword);
  }

  @Override
  public void read(OChannelBinary channel, int protocolVersion, String serializerName) throws IOException {
    rootUser = channel.readString();
    rootPassword = channel.readString();
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_SHUTDOWN;
  }

  public String getRootPassword() {
    return rootPassword;
  }

  public String getRootUser() {
    return rootUser;
  }

}