package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.binary.OBinaryRequestExecutor;
import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

public class OUnlockRecordRequest implements OBinaryRequest<OUnlockRecordResponse> {
  private ORID identity;

  public OUnlockRecordRequest() {}

  public OUnlockRecordRequest(ORID identity) {
    this.identity = identity;
  }

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    network.writeRID(identity);
  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    this.identity = channel.readRID();
  }

  @Override
  public byte getCommand() {
    return OExperimentalRequest.REQUEST_RECORD_UNLOCK;
  }

  @Override
  public OUnlockRecordResponse createResponse() {
    return new OUnlockRecordResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeUnlockRecord(this);
  }

  @Override
  public String getDescription() {
    return "Unlock Record";
  }

  public ORID getIdentity() {
    return identity;
  }
}
