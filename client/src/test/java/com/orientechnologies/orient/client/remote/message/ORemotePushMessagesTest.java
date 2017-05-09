package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
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

}
