package com.orientechnologies.orient.server.distributed.impl.task;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class OCloseQueryTaskTest extends AbstractRemoteTaskTest{

  @Test
  public void testSerialization() throws IOException {
    OCloseQueryTask from = new OCloseQueryTask("foo");
    OCloseQueryTask to = new OCloseQueryTask();
    serializeDeserialize(from, to);

    Assert.assertEquals(from.getQueryId(), to.getQueryId());
  }
}
