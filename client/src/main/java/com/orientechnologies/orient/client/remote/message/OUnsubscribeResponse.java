package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

/** Created by tglman on 19/06/17. */
public class OUnsubscribeResponse implements OBinaryResponse {

  private OBinaryResponse response;

  public OUnsubscribeResponse(OBinaryResponse response) {
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
}
