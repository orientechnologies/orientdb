package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

public class OSetGlobalConfigurationRequest
    implements OBinaryRequest<OSetGlobalConfigurationResponse> {
  private String key;
  private String value;

  public OSetGlobalConfigurationRequest(String config, String iValue) {
    key = config;
    value = iValue;
  }

  public OSetGlobalConfigurationRequest() {}

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    network.writeString(key);
    network.writeString(value);
  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    key = channel.readString();
    value = channel.readString();
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_CONFIG_SET;
  }

  @Override
  public String getDescription() {
    return "Set config";
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  public OSetGlobalConfigurationResponse createResponse() {
    return new OSetGlobalConfigurationResponse();
  }

  @Override
  public boolean requireDatabaseSession() {
    return false;
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeSetGlobalConfig(this);
  }
}
