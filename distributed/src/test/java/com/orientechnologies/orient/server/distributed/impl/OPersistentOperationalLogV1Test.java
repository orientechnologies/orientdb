package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.server.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.server.distributed.impl.coordinator.OLogId;
import com.orientechnologies.orient.server.distributed.impl.coordinator.ONodeRequest;
import com.orientechnologies.orient.server.distributed.impl.coordinator.OOperationLogEntry;
import com.orientechnologies.orient.server.distributed.impl.coordinator.mocktx.OPhase1Tx;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
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

  @Test
  public void testReadRecord() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Path file = Files.createTempDirectory(".");
    OPersistentOperationalLogV1 log = new OPersistentOperationalLogV1(file.toString()) {
      protected OCoordinateMessagesFactory getCoordinateMessagesFactory() {
        return new OCoordinateMessagesFactory() {
          @Override
          public ONodeRequest createOperationRequest(int requestType) {
            return new OPhase1Tx();
          }
        };
      }
    };

    try {

      DataOutputStream outStream = new DataOutputStream(out);
      for (int i = 0; i < 3; i++) {
        OPhase1Tx item = new OPhase1Tx();
        OLogId id = log.log(item);
        log.writeRecord(outStream, id, item);
      }
      ByteArrayInputStream input = new ByteArrayInputStream(out.toByteArray());
      DataInputStream inStream = new DataInputStream(input);
      for (int i = 0; i < 3; i++) {
        OOperationLogEntry item = log.readRecord(inStream);
        Assert.assertEquals(i, item.getLogId().getId());
      }

      OOperationLogEntry next = log.readRecord(inStream);
      Assert.assertNull(next);

    } finally {
      for (File file1 : file.toFile().listFiles()) {
        file1.delete();
      }
      file.toFile().delete();
    }
  }

}
