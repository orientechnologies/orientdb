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

/** Created by tglman on 19/06/17. */
public class OUnsubscribeLiveQueryRequest implements OBinaryRequest<OUnsubscribLiveQueryResponse> {

  private int monitorId;

  public OUnsubscribeLiveQueryRequest() {}

  public OUnsubscribeLiveQueryRequest(int monitorId) {
    this.monitorId = monitorId;
  }

  @Override
  public void write(OChannelDataOutput network, OStorageRemoteSession session) throws IOException {
    network.writeInt(monitorId);
  }

  @Override
  public void read(OChannelDataInput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    monitorId = channel.readInt();
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.UNSUBSCRIBE_PUSH_LIVE_QUERY;
  }

  @Override
  public OUnsubscribLiveQueryResponse createResponse() {
    return new OUnsubscribLiveQueryResponse();
  }

  @Override
  public OBinaryResponse execute(OBinaryRequestExecutor executor) {
    return executor.executeUnsubscribeLiveQuery(this);
  }

  public int getMonitorId() {
    return monitorId;
  }

  @Override
  public String getDescription() {
    return "unsubscribe from live query";
  }
}
