package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.server.distributed.impl.coordinator.OLogId;
import com.orientechnologies.orient.server.distributed.impl.coordinator.mocktx.OPhase1Tx;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

public class OPersistentOperationalLogV1Test {

  @Test
  public void testReadLastId() throws IOException {
    Path file = Files.createTempDirectory(".");
    try {
      OPersistentOperationalLogV1 oplog = new OPersistentOperationalLogV1(file.toString());
      OLogId logId = null;
      for (int i = 0; i < 10; i++) {
        OPhase1Tx req = new OPhase1Tx();
        logId = oplog.log(req);
        oplog.logReceived(logId, req);
      }
      oplog.close();
      oplog = new OPersistentOperationalLogV1(file.toString());
      AtomicLong lastLogId = oplog.readLastLogId();
      Assert.assertEquals(logId.getId(), lastLogId.get());
    } finally {

      for (File file1 : file.toFile().listFiles()) {
        file1.delete();
      }
      file.toFile().delete();
    }
  }
}
