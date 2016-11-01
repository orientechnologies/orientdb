package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;

import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinary;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

public class OGetGlobalConfigurationRequest implements OBinaryRequest<OGetGlobalConfigurationResponse> {
  private String key;

  public OGetGlobalConfigurationRequest(String key) {
    this.key = key;
  }

  public OGetGlobalConfigurationRequest() {
  }

  @Override
  public void write(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    network.writeString(key);
  }

  @Override
  public void read(OChannelBinary channel, int protocolVersion, String serializerName) throws IOException {
    key = channel.readString();
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_CONFIG_GET;
  }

  public String getKey() {
    return key;
  }

  @Override
  public OGetGlobalConfigurationResponse createResponse() {
    return new OGetGlobalConfigurationResponse();
  }

}