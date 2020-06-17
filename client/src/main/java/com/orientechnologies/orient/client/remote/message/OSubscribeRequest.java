package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

/** Created by tglman on 16/05/17. */
public class OSubscribeRequest implements OBinaryRequest<OSubscribeResponse> {

  private byte pushMessage;
  private OBinaryRequest<? extends OBinaryResponse> pushRequest;

  public OSubscribeRequest() {}

  public OSubscribeRequest(OBinaryRequest<? extends OBinaryResponse> request) {
    this.pushMessage = request.getCommand();
    this.pushRequest = request;
  }

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    network.writeByte(pushMessage);
    pushRequest.write(network, session);
  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    pushMessage = channel.readByte();
    pushRequest = createBinaryRequest(pushMessage);
    pushRequest.read(channel, protocolVersion, serializer);
  }

  private OBinaryRequest<? extends OBinaryResponse> createBinaryRequest(byte message) {
    switch (message) {
      case OChannelBinaryProtocol.SUBSCRIBE_PUSH_DISTRIB_CONFIG:
        return new OSubscribeDistributedConfigurationRequest();
      case OChannelBinaryProtocol.SUBSCRIBE_PUSH_LIVE_QUERY:
        return new OSubscribeLiveQueryRequest();
      case OChannelBinaryProtocol.SUBSCRIBE_PUSH_STORAGE_CONFIG:
        return new OSubscribeStorageConfigurationRequest();
      case OChannelBinaryProtocol.SUBSCRIBE_PUSH_SCHEMA:
        return new OSubscribeSchemaRequest();
      case OChannelBinaryProtocol.SUBSCRIBE_PUSH_INDEX_MANAGER:
        return new OSubscribeIndexManagerRequest();
      case OChannelBinaryProtocol.SUBSCRIBE_PUSH_FUNCTIONS:
        return new OSubscribeFunctionsRequest();
      case OChannelBinaryProtocol.SUBSCRIBE_PUSH_SEQUENCES:
        return new OSubscribeSequencesRequest();
    }

    throw new ODatabaseException("Unknown message response for code:" + message);
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.SUBSCRIBE_PUSH;
  }

  @Override
  public OSubscribeResponse createResponse() {
    return new OSubscribeResponse(pushRequest.createResponse());
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeSubscribe(this);
  }

  public byte getPushMessage() {
    return pushMessage;
  }

  public OBinaryRequest<? extends OBinaryResponse> getPushRequest() {
    return pushRequest;
  }

  @Override
  public String getDescription() {
    return "Subscribe to push message";
  }
}
