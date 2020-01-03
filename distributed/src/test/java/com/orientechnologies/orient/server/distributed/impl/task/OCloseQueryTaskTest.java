package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;

public class OCloseQueryTaskTest extends AbstractRemoteTaskTest{

  @Test
  public void testSerialization() throws IOException {
    OCloseQueryTask from = new OCloseQueryTask("foo");
    OCloseQueryTask to = new OCloseQueryTask();
    serializeDeserialize(from, to);

    Assert.assertEquals(from.getQueryId(), to.getQueryId());
  }
}
