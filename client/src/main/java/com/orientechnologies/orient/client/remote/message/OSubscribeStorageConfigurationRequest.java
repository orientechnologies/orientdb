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

/**
 * Created by tglman on 11/01/17.
 */
public class OSubscribeStorageConfigurationRequest implements OBinaryRequest<OSubscribeStorageConfigurationResponse> {
  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {

  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer) throws IOException {

  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.SUBSCRIBE_PUSH_STORAGE_CONFIG;
  }

  @Override
  public OSubscribeStorageConfigurationResponse createResponse() {
    return new OSubscribeStorageConfigurationResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeSubscribeStorageConfiguration(this);
  }

  @Override
  public String getDescription() {
    return "Subscribe Distributed Configuration";
  }

}
