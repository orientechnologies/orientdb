package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkFactory;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import java.io.IOException;
import java.util.*;
import org.junit.Assert;
import org.junit.Test;

/** Created by luigidellaquila on 14/12/16. */
public class OServerQueryResponseTest {

  @Test
  public void test() throws IOException {

    List<OResultInternal> resuls = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      OResultInternal item = new OResultInternal();
      item.setProperty("name", "foo");
      item.setProperty("counter", i);
      resuls.add(item);
    }
    OServerQueryResponse response =
        new OServerQueryResponse(
            "query", true, resuls, Optional.empty(), false, new HashMap<>(), true);

    MockChannel channel = new MockChannel();
    response.write(
        channel,
        OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION,
        ORecordSerializerNetworkFactory.INSTANCE.current());

    channel.close();

    OServerQueryResponse newResponse = new OServerQueryResponse();

    newResponse.read(channel, null);
    Iterator<OResultInternal> responseRs = newResponse.getResult().iterator();

    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(responseRs.hasNext());
      OResult item = responseRs.next();
      Assert.assertEquals("foo", item.getProperty("name"));
      Assert.assertEquals((Integer) i, item.getProperty("counter"));
    }
    Assert.assertFalse(responseRs.hasNext());
    Assert.assertTrue(newResponse.isReloadMetadata());
    Assert.assertTrue(newResponse.isTxChanges());
  }
}
