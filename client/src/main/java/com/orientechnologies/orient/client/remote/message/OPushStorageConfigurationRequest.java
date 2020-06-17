package com.orientechnologies.orient.client.remote.message;

import static com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol.REQUEST_PUSH_STORAGE_CONFIG;

import com.orientechnologies.orient.client.remote.ORemotePushHandler;
import com.orientechnologies.orient.client.remote.message.push.OStorageConfigurationPayload;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

public class OPushStorageConfigurationRequest implements OBinaryPushRequest<OBinaryPushResponse> {

  private OStorageConfigurationPayload payload;

  public OPushStorageConfigurationRequest() {
    payload = new OStorageConfigurationPayload();
  }

  public OPushStorageConfigurationRequest(OStorageConfiguration configuration) {
    payload = new OStorageConfigurationPayload(configuration);
  }

  @Override
  public void write(OChannelDataOutput channel) throws IOException {
    payload.write(channel);
  }

  @Override
  public void read(OChannelDataInput network) throws IOException {
    payload.read(network);
  }

  @Override
  public OBinaryPushResponse execute(ORemotePushHandler pushHandler) {
    return pushHandler.executeUpdateStorageConfig(this);
  }

  @Override
  public OBinaryPushResponse createResponse() {
    return null;
  }

  @Override
  public byte getPushCommand() {
    return REQUEST_PUSH_STORAGE_CONFIG;
  }

  public OStorageConfigurationPayload getPayload() {
    return payload;
  }
}
