package com.orientechnologies.orient.server.distributed.impl.task;

import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class OFetchQueryPageTaskTest extends AbstractRemoteTaskTest {

  @Test
  public void testSerialization() throws IOException {
    OFetchQueryPageTask from = new OFetchQueryPageTask("foo");
    OFetchQueryPageTask to = new OFetchQueryPageTask();
    serializeDeserialize(from, to);

    Assert.assertEquals(from.getQueryId(), to.getQueryId());
  }
}
