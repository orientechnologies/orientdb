package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.id.ORecordId;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class OCreateRecordTaskTest extends AbstractRemoteTaskTest {

  @Test
  public void testSerialization() throws IOException {
    OCreateRecordTask from = new OCreateRecordTask();
    from.init(new ORecordId(12, 0), "foo".getBytes(), 1, (byte) 0);
    OCreateRecordTask to = new OCreateRecordTask();
    serializeDeserialize(from, to);

    Assert.assertEquals(from.getRid(), to.getRid());
    Assert.assertArrayEquals(from.getContent(), to.getContent());
    Assert.assertEquals(from.getVersion(), to.getVersion());
    Assert.assertEquals(from.getRecordType(), to.getRecordType());
  }
}
