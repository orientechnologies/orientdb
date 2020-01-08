package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.id.ORecordId;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class ODeleteRecordTaskTest extends AbstractRemoteTaskTest {

  @Test
  public void testSerialization() throws IOException {
    ODeleteRecordTask from = new ODeleteRecordTask();
    from.init(new ORecordId(12, 0), 1);
    ODeleteRecordTask to = new ODeleteRecordTask();
    serializeDeserialize(from, to);

    Assert.assertEquals(from.getRid(), to.getRid());
    Assert.assertEquals(from.getVersion(), to.getVersion());
  }
}
