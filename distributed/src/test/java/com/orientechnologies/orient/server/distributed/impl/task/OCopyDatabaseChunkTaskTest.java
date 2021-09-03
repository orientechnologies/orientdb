package com.orientechnologies.orient.server.distributed.impl.task;

import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class OCopyDatabaseChunkTaskTest extends AbstractRemoteTaskTest {

  @Test
  public void testSerialization() throws IOException {
    OCopyDatabaseChunkTask from = new OCopyDatabaseChunkTask("foo1", 10, 20L, false);
    OCopyDatabaseChunkTask to = new OCopyDatabaseChunkTask();
    serializeDeserialize(from, to);

    Assert.assertEquals(from.getFileName(), to.getFileName());
    Assert.assertEquals(from.getChunkNum(), to.getChunkNum());
    Assert.assertEquals(from.getOffset(), to.getOffset());
    Assert.assertEquals(from.isCompressed(), to.isCompressed());
  }
}
