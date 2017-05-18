package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.client.remote.ORemotePushHandler;
import com.orientechnologies.orient.client.remote.message.live.OLiveQueryResult;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by tglman on 17/05/17.
 */
public class OLiveQueryPushRequest implements OBinaryPushRequest {

  public static final byte HAS_MORE = 1;
  public static final byte END      = 2;

  private long                   monitorId;
  private byte                   status;
  private List<OLiveQueryResult> events;

  public OLiveQueryPushRequest(long monitorId, byte status, List<OLiveQueryResult> events) {
    this.monitorId = monitorId;
    this.status = status;
    this.events = events;
  }

  public OLiveQueryPushRequest() {

  }

  @Override
  public void write(OChannelDataOutput channel) throws IOException {
    channel.writeLong(monitorId);
    channel.writeByte(status);
    channel.writeInt(events.size());
    for (OLiveQueryResult event : events) {
      channel.writeByte(event.getEventType());
      OMessageHelper.writeResult(event.getCurrentValue(), channel, ORecordSerializerNetworkV37.INSTANCE);
      if (event.getEventType() == OLiveQueryResult.UPDATE_EVENT) {
        OMessageHelper.writeResult(event.getOldValue(), channel, ORecordSerializerNetworkV37.INSTANCE);
      }
    }
  }

  @Override
  public void read(OChannelDataInput network) throws IOException {
    monitorId = network.readLong();
    status = network.readByte();
    int eventSize = network.readInt();
    events = new ArrayList<>(eventSize);
    while (eventSize-- > 0) {
      byte type = network.readByte();
      OResult currentValue = OMessageHelper.readResult(network);
      OResult oldValue = null;
      if (type == OLiveQueryResult.UPDATE_EVENT) {
        oldValue = OMessageHelper.readResult(network);
      }
      events.add(new OLiveQueryResult(type, currentValue, oldValue));
    }
  }

  @Override
  public OBinaryPushResponse execute(ORemotePushHandler remote) {
    remote.executeLiveQueryPush(this);
    return null;
  }

  @Override
  public OBinaryPushResponse createResponse() {
    return null;
  }

  @Override
  public byte getPushCommand() {
    return OChannelBinaryProtocol.REQUEST_PUSH_LIVE_QUERY;
  }

  public long getMonitorId() {
    return monitorId;
  }

  public List<OLiveQueryResult> getEvents() {
    return events;
  }

  public byte getStatus() {
    return status;
  }

  public void setStatus(byte status) {
    this.status = status;
  }
}
