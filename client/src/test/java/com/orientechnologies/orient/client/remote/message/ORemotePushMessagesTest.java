package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by tglman on 09/05/17.
 */
public class ORemotePushMessagesTest {

  @Test
  public void testDistributedConfig() throws IOException {
    MockChannel channel = new MockChannel();
    List<String> hosts = new ArrayList<>();
    hosts.add("one");
    hosts.add("two");
    OPushDistributedConfigurationRequest request = new OPushDistributedConfigurationRequest(hosts);
    request.write(channel);
    channel.close();

    OPushDistributedConfigurationRequest readRequest = new OPushDistributedConfigurationRequest();
    readRequest.read(channel);
    assertEquals(readRequest.getHosts().size(), 2);
    assertEquals(readRequest.getHosts().get(0), "one");
    assertEquals(readRequest.getHosts().get(1), "two");
  }

  @Test
  public void testSubscribeRequest() throws IOException {
    MockChannel channel = new MockChannel();

    OSubscribeRequest request = new OSubscribeRequest(new OSubscribeLiveQueryRequest("10", new HashMap<>()));
    request.write(channel, null);
    channel.close();

    OSubscribeRequest requestRead = new OSubscribeRequest();
    requestRead.read(channel, 1, ORecordSerializerNetworkV37.INSTANCE);

    assertEquals(request.getPushMessage(), requestRead.getPushMessage());
    assertTrue(requestRead.getPushRequest() instanceof OSubscribeLiveQueryRequest);

  }

  @Test
  public void testSubscribeResponse() throws IOException {
    MockChannel channel = new MockChannel();

    OSubscribeResponse response = new OSubscribeResponse(new OSubscribeLiveQueryResponse(10));
    response.write(channel, 1, ORecordSerializerNetworkV37.INSTANCE);
    channel.close();

    OSubscribeResponse responseRead = new OSubscribeResponse(new OSubscribeLiveQueryResponse());
    responseRead.read(channel, null);

    assertTrue(responseRead.getResponse() instanceof OSubscribeLiveQueryResponse);
    assertEquals(((OSubscribeLiveQueryResponse) responseRead.getResponse()).getMonitorId(), 10);

  }

  @Test
  public void testUnsubscribeRequest() throws IOException {
    MockChannel channel = new MockChannel();
    OUnsubscribeRequest request = new OUnsubscribeRequest(new OUnsubscribeLiveQueryRequest(10));
    request.write(channel, null);
    channel.close();
    OUnsubscribeRequest readRequest = new OUnsubscribeRequest();
    readRequest.read(channel, 0, null);
    assertEquals(((OUnsubscribeLiveQueryRequest) readRequest.getUnsubscribeRequest()).getMonitorId(), 10);
  }

}
