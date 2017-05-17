package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by tglman on 17/05/17.
 */
public class OLiveQuerySubscribeMessagesTests {

  @Test
  public void testRequestWriteRead() throws IOException {
    Map<String, Object> params = new HashMap<>();
    params.put("par", "value");
    OSubscribeLiveQueryRequest request = new OSubscribeLiveQueryRequest("select from Some", params);
    MockChannel channel = new MockChannel();
    request.write(channel, null);
    channel.close();
    OSubscribeLiveQueryRequest requestRead = new OSubscribeLiveQueryRequest();
    requestRead.read(channel, -1, new ORecordSerializerNetworkV37());
    assertEquals(requestRead.getQuery(), "select from Some");
    assertEquals(requestRead.getParams(), params);
  }

  @Test
  public void testSubscribeResponseWriteRead() throws IOException {
    OSubscribeLiveQueryResponse response = new OSubscribeLiveQueryResponse(20);
    MockChannel channel = new MockChannel();
    response.write(channel, 0, null);
    channel.close();
    OSubscribeLiveQueryResponse responseRead = new OSubscribeLiveQueryResponse();
    responseRead.read(channel, null);
    assertEquals(responseRead.getMonitorId(), 20);
  }
}
