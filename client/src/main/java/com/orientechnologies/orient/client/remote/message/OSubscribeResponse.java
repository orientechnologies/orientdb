package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

/** Created by tglman on 16/05/17. */
public class OSubscribeResponse implements OBinaryResponse {

  private OBinaryResponse response;

  public OSubscribeResponse() {}

  public OSubscribeResponse(OBinaryResponse response) {
    this.response = response;
  }

  @Override
  public void write(OChannelDataOutput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    response.write(channel, protocolVersion, serializer);
  }

  @Override
  public void read(OChannelDataInput network, OStorageRemoteSession session) throws IOException {
    response.read(network, session);
  }

  public OBinaryResponse getResponse() {
    return response;
  }
}
