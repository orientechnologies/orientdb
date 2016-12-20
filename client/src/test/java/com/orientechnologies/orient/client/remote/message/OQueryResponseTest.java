package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetwork;
import com.orientechnologies.orient.core.sql.executor.OInternalResultSet;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.parser.OLocalResultSetLifecycleDecorator;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by luigidellaquila on 14/12/16.
 */
public class OQueryResponseTest {

  @Test public void test() throws IOException {
    OQueryResponse response = new OQueryResponse();
    OInternalResultSet rs = new OInternalResultSet();
    for (int i = 0; i < 10; i++) {
      OResultInternal item = new OResultInternal();
      item.setProperty("name", "foo");
      item.setProperty("counter", i);
      rs.add(item);
    }
    response.setResult(new OLocalResultSetLifecycleDecorator(rs));

    MockChannelDataOut channel = new MockChannelDataOut();
    response.write(channel, OChannelBinaryProtocol.CURRENT_PROTOCOL_VERSION, ORecordSerializerNetwork.INSTANCE);

    channel.close();

    OQueryResponse newResponse = new OQueryResponse();

    ORemoteResultSet responseRs = new ORemoteResultSet(null);
    newResponse.doRead(channel, responseRs);

    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(responseRs.hasNext());
      OResult item = responseRs.next();
      Assert.assertEquals("foo", item.getProperty("name"));
      Assert.assertEquals((Integer)i, item.getProperty("counter"));
    }
    Assert.assertFalse(responseRs.hasNext());
  }

}
