package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class OCompleted2pcTaskTest extends AbstractRemoteTaskTest {

  @Test
  public void testSerialization() throws IOException {
    OCompleted2pcTask from = new OCompleted2pcTask();
    from.init(new ODistributedRequestId(10, 1000), true, new int[]{1, 2, 3});
    from.addFixTask(new OReadRecordTask().init(new ORecordId(12, 0)));
    OCompleted2pcTask to = new OCompleted2pcTask();
    serializeDeserialize(from, to);

    Assert.assertEquals(from.getRequestId(), to.getRequestId());
    Assert.assertEquals(from.getFixTasks().size(), to.getFixTasks().size());
    Assert.assertEquals(((OReadRecordTask) from.getFixTasks().get(0)).getRid(), ((OReadRecordTask) to.getFixTasks().get(0)).getRid());
  }
}
