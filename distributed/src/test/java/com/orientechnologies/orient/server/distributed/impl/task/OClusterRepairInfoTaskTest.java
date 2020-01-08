package com.orientechnologies.orient.server.distributed.impl.task;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class OClusterRepairInfoTaskTest extends AbstractRemoteTaskTest {

  @Test
  public void testSerialization() throws IOException {
    OClusterRepairInfoTask from = new OClusterRepairInfoTask(100);
    OClusterRepairInfoTask to = new OClusterRepairInfoTask();
    serializeDeserialize(from, to);

    Assert.assertEquals(from.getClusterId(), to.getClusterId());
  }
}
