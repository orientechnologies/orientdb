package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class OCompleted2pcTaskV1Test extends AbstractRemoteTaskTest {

  @Test
  public void testSerialization() throws IOException {
    OCompleted2pcTaskV1 from = new OCompleted2pcTaskV1();
    from.init(new ODistributedRequestId(10, 1000), true, new int[]{1, 2, 3});
    from.addFixTask(new OReadRecordTask().init(new ORecordId(12, 0)));
    OCompleted2pcTaskV1 to = new OCompleted2pcTaskV1();
    serializeDeserialize(from, to);

    Assert.assertEquals(from.getRequestId(), to.getRequestId());
    Assert.assertEquals(from.getSuccess(), to.getSuccess());
    Assert.assertArrayEquals(from.getPartitionKey(), to.getPartitionKey());
    Assert.assertEquals(from.getFixTasks().size(), to.getFixTasks().size());
    Assert.assertEquals(((OReadRecordTask) from.getFixTasks().get(0)).getRid(), ((OReadRecordTask) to.getFixTasks().get(0)).getRid());
  }
}
