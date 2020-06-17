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
public class OExperimentalRequest implements OBinaryRequest<OExperimentalResponse> {

  public static final byte REQUEST_RECORD_LOCK = 48;
  public static final byte REQUEST_RECORD_UNLOCK = 49;
  private byte messageID;
  private OBinaryRequest<? extends OBinaryResponse> request;

  public OExperimentalRequest() {}

  public OExperimentalRequest(OBinaryRequest<? extends OBinaryResponse> request) {
    this.messageID = request.getCommand();
    this.request = request;
  }

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    network.writeByte(messageID);
    request.write(network, session);
  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    messageID = channel.readByte();
    request = createBinaryRequest(messageID);
    request.read(channel, protocolVersion, serializer);
  }

  private OBinaryRequest<? extends OBinaryResponse> createBinaryRequest(byte message) {
    switch (message) {
      case REQUEST_RECORD_LOCK:
        return new OLockRecordRequest();
      case REQUEST_RECORD_UNLOCK:
        return new OUnlockRecordRequest();
        // NONE FOR NOW
    }

    throw new ODatabaseException("Unknown message response for code:" + message);
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.EXPERIMENTAL;
  }

  @Override
  public OExperimentalResponse createResponse() {
    return new OExperimentalResponse(request.createResponse());
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeExperimental(this);
  }

  public byte getMessageID() {
    return messageID;
  }

  public OBinaryRequest<? extends OBinaryResponse> getRequest() {
    return request;
  }

  @Override
  public String getDescription() {
    return "Experimental message:" + (request == null ? "Not Defined" : request.getDescription());
  }
}
