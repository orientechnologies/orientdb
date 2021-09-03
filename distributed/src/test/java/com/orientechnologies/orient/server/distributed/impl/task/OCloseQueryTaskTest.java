package com.orientechnologies.orient.server.distributed.impl.task;

import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class OCloseQueryTaskTest extends AbstractRemoteTaskTest {

  @Test
  public void testSerialization() throws IOException {
    OCloseQueryTask from = new OCloseQueryTask("foo");
    OCloseQueryTask to = new OCloseQueryTask();
    serializeDeserialize(from, to);

    Assert.assertEquals(from.getQueryId(), to.getQueryId());
  }
}
