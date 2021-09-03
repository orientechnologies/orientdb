package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;

/** Created by tglman on 17/05/17. */
public class OSubscribeLiveQueryResponse implements OBinaryResponse {

  private int monitorId;

  public OSubscribeLiveQueryResponse(int monitorId) {
    this.monitorId = monitorId;
  }

  public OSubscribeLiveQueryResponse() {}

  @Override
  public void write(OChannelDataOutput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    channel.writeInt(monitorId);
  }

  @Override
  public void read(OChannelDataInput network, OStorageRemoteSession session) throws IOException {
    monitorId = network.readInt();
  }

  public int getMonitorId() {
    return monitorId;
  }
}
