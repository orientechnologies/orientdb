package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkFactory;
import com.orientechnologies.orient.core.sql.executor.OInternalResultSet;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.parser.OLocalResultSetLifecycleDecorator;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

/**
 * Created by luigidellaquila on 14/12/16.
 */
public class OQueryResponseTest {

  @Test
  public void test() throws IOException {

    List<OResult> resuls = new ArrayList<OResult>();
    for (int i = 0; i < 10; i++) {
      OResultInternal item = new OResultInternal();
      item.setProperty("name", "foo");
      item.setProperty("counter", i);
      resuls.add(item);
    }
    OQueryResponse response = new OQueryResponse("query", false, resuls, Optional.empty(), false, new HashMap<>());

    MockChannel channel = new MockChannel();
    response.write(channel, OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION, ORecordSerializerNetworkFactory.INSTANCE.current());

    channel.close();

    OQueryResponse newResponse = new OQueryResponse();

    newResponse.read(channel, null);
    Iterator<OResult> responseRs = newResponse.getResult().iterator();

    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(responseRs.hasNext());
      OResult item = responseRs.next();
      Assert.assertEquals("foo", item.getProperty("name"));
      Assert.assertEquals((Integer) i, item.getProperty("counter"));
    }
    Assert.assertFalse(responseRs.hasNext());
  }

}
