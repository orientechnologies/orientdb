package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;

public class OGetServerInfoResponse implements OBinaryResponse<String> {
  private String result;

  public OGetServerInfoResponse(String result) {
    this.result = result;
  }

  public OGetServerInfoResponse() {
  }

  @Override
  public void write(OChannelBinary channel, int protocolVersion, String recordSerializer) throws IOException {
    channel.writeString(result);
  }

  @Override
  public String read(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    result = network.readString();
    return result;
  }

  public String getResult() {
    return result;
  }
}