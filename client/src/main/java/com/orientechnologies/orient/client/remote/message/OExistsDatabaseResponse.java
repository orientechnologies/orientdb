package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;

public class OExistsDatabaseResponse implements OBinaryResponse<Boolean> {
  private boolean exists;

  public OExistsDatabaseResponse() {
  }

  public OExistsDatabaseResponse(boolean exists) {
    this.exists = exists;
  }

  @Override
  public void write(OChannelBinary channel, int protocolVersion, String recordSerializer) throws IOException {
    channel.writeBoolean(exists);
  }

  @Override
  public Boolean read(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    exists = network.readBoolean();
    return exists;
  }

  public boolean isExists() {
    return exists;
  }
}