package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;

public class OServerInfoResponse implements OBinaryResponse {
  private String result;

  public OServerInfoResponse(String result) {
    this.result = result;
  }

  public OServerInfoResponse() {
  }

  @Override
  public void write(OChannelBinary channel, int protocolVersion, String recordSerializer) throws IOException {
    channel.writeString(result);
  }

  @Override
  public void read(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    result = network.readString();
  }

  public String getResult() {
    return result;
  }
}