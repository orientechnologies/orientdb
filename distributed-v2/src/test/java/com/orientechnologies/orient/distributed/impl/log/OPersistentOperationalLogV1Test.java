package com.orientechnologies.orient.distributed.impl.log;

import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.distributed.impl.coordinator.mocktx.OPhase1Tx;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class OPersistentOperationalLogV1Test {

  @Test
  public void testReadLastId() throws IOException {
    OCoordinateMessagesFactory factory = new OCoordinateMessagesFactory();
    Path file = Files.createTempDirectory(".");
    try {
      OPersistentOperationalLogV1 oplog =
          new OPersistentOperationalLogV1(
              file.toString(), (id) -> factory.createOperationRequest(id));
      oplog.setLeader(true, 0);
      OLogId logId = null;
      for (int i = 0; i < 10; i++) {
        OPhase1Tx req = new OPhase1Tx();
        logId = oplog.log(req);
      }
      oplog.close();
      oplog =
          new OPersistentOperationalLogV1(
              file.toString(), (id) -> factory.createOperationRequest(id));
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
    OPersistentOperationalLogV1 log =
        new OPersistentOperationalLogV1(file.toString(), (id) -> new OPhase1Tx());

    try {

      DataOutputStream outStream = new DataOutputStream(out);
      for (int i = 0; i < 3; i++) {
        OPhase1Tx item = new OPhase1Tx();
        OLogId id = log.createLogId();
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

  @Test
  public void testIterate() throws IOException {

    Path file = Files.createTempDirectory(".");
    OPersistentOperationalLogV1 log =
        new OPersistentOperationalLogV1(file.toString(), (id) -> new OPhase1Tx());
    log.setLeader(true, 0);
    try {
      int totLogEntries = 500;
      for (int i = 0; i < totLogEntries; i++) {
        OPhase1Tx item = new OPhase1Tx();
        log.log(item);
      }

      long nextEntry = 100;
      OOplogIterator iteartor = log.iterate(nextEntry, totLogEntries - 1);
      try {
        while (nextEntry < totLogEntries) {
          OOperationLogEntry item = iteartor.next();
          Assert.assertEquals(nextEntry++, item.getLogId().getId());
        }
        Assert.assertFalse(iteartor.hasNext());
      } finally {
        iteartor.close();
      }
    } finally {
      for (File file1 : file.toFile().listFiles()) {
        file1.delete();
      }
      file.toFile().delete();
    }
  }

  @Test
  public void testIterate2() throws IOException {

    Path file = Files.createTempDirectory(".");
    OPersistentOperationalLogV1 log =
        new OPersistentOperationalLogV1(file.toString(), (id) -> new OPhase1Tx());
    log.setLeader(true, 0);
    try {
      int totLogEntries = 500;

      for (int i = 0; i < totLogEntries; i++) {
        OPhase1Tx item = new OPhase1Tx();
        log.log(item);
      }

      OOplogIterator iteartor = log.iterate(100, 400 - 1);
      for (int i = 100; i < 400; i++) {
        OOperationLogEntry item = iteartor.next();
        Assert.assertEquals(i, item.getLogId().getId());
      }
      Assert.assertFalse(iteartor.hasNext());
      iteartor.close();

    } finally {
      for (File file1 : file.toFile().listFiles()) {
        file1.delete();
      }
      file.toFile().delete();
    }
  }

  @Test
  public void testRemoveAfter1() throws IOException {
    Path file = Files.createTempDirectory(".");
    OPersistentOperationalLogV1 log =
        new OPersistentOperationalLogV1(file.toString(), (id) -> new OPhase1Tx());
    log.setLeader(true, 0);
    try {
      int totLogEntries = 100;

      for (int i = 0; i < totLogEntries; i++) {
        OPhase1Tx item = new OPhase1Tx();
        log.log(item);
      }
      OOperationLog.LogIdStatus status = log.removeAfter(new OLogId(10, 0, 0));

      Assert.assertEquals(OOperationLog.LogIdStatus.PRESENT, status);

      OOplogIterator iteartor = log.iterate(0, 100);
      for (int i = 0; i <= 10; i++) {
        OOperationLogEntry item = iteartor.next();
        Assert.assertEquals(i, item.getLogId().getId());
      }
      Assert.assertFalse(iteartor.hasNext());
      iteartor.close();

      for (int i = 0; i < 5; i++) {
        OPhase1Tx item = new OPhase1Tx();
        log.log(item);
      }

      iteartor = log.iterate(0, 100);
      for (int i = 0; i <= 15; i++) {
        OOperationLogEntry item = iteartor.next();
        Assert.assertEquals(i, item.getLogId().getId());
      }
      Assert.assertFalse(iteartor.hasNext());

      iteartor.close();

    } finally {
      for (File file1 : file.toFile().listFiles()) {
        file1.delete();
      }
      file.toFile().delete();
    }
  }

  @Test
  public void testRemoveAfter2() throws IOException {
    Path file = Files.createTempDirectory(".");
    OPersistentOperationalLogV1 log =
        new OPersistentOperationalLogV1(file.toString(), (id) -> new OPhase1Tx());
    log.setLeader(true, 0);
    try {
      int totLogEntries = 100;

      for (int i = 0; i < totLogEntries; i++) {
        OPhase1Tx item = new OPhase1Tx();
        log.log(item);
      }
      OOperationLog.LogIdStatus status = log.removeAfter(new OLogId(100, 0, 0));

      Assert.assertEquals(OOperationLog.LogIdStatus.FUTURE, status);

      OOplogIterator iteartor = log.iterate(0, 100);
      for (int i = 0; i < 100; i++) {
        OOperationLogEntry item = iteartor.next();
        Assert.assertEquals(i, item.getLogId().getId());
      }
      Assert.assertFalse(iteartor.hasNext());
      iteartor.close();

    } finally {
      for (File file1 : file.toFile().listFiles()) {
        file1.delete();
      }
      file.toFile().delete();
    }
  }

  @Test
  public void testRemoveAfter3() throws IOException {
    Path file = Files.createTempDirectory(".");
    OPersistentOperationalLogV1 log =
        new OPersistentOperationalLogV1(file.toString(), (id) -> new OPhase1Tx());
    log.setLeader(true, 0);

    try {
      int totLogEntries = 100;

      for (int i = 0; i < totLogEntries; i++) {
        OPhase1Tx item = new OPhase1Tx();
        log.log(item);
      }
      OOperationLog.LogIdStatus status = log.removeAfter(new OLogId(-5, 0, 0));

      Assert.assertEquals(OOperationLog.LogIdStatus.TOO_OLD, status);

      OOplogIterator iteartor = log.iterate(0, 100);

      Assert.assertFalse(iteartor.hasNext());
      iteartor.close();

    } finally {
      for (File file1 : file.toFile().listFiles()) {
        file1.delete();
      }
      file.toFile().delete();
    }
  }

  @Test
  public void testRemoveAfter4() throws IOException {
    Path file = Files.createTempDirectory(".");
    OPersistentOperationalLogV1 log =
        new OPersistentOperationalLogV1(file.toString(), (id) -> new OPhase1Tx());
    log.setLeader(true, 0);

    try {
      int totLogEntries = 100;

      for (int i = 0; i < totLogEntries; i++) {
        OPhase1Tx item = new OPhase1Tx();
        log.log(item);
      }
      OOperationLog.LogIdStatus status =
          log.removeAfter(
              new OLogId(10, 0, 0) {
                @Override
                public boolean equals(Object o) {
                  return false;
                }
              });

      Assert.assertEquals(OOperationLog.LogIdStatus.INVALID, status);

      OOplogIterator iteartor = log.iterate(0, 100);
      try {
        for (int i = 0; i < 100; i++) {
          OOperationLogEntry item = iteartor.next();
          Assert.assertEquals(i, item.getLogId().getId());
        }
        Assert.assertFalse(iteartor.hasNext());
      } finally {
        iteartor.close();
      }

    } finally {
      for (File file1 : file.toFile().listFiles()) {
        file1.delete();
      }
      file.toFile().delete();
    }
  }

  @Test
  @Ignore
  public void stressTest() throws IOException {

    Path file = Files.createTempDirectory(".");
    OPersistentOperationalLogV1 log =
        new OPersistentOperationalLogV1(file.toString(), (id) -> new OPhase1Tx());
    log.setLeader(true, 0);
    try {
      final long totLogEntries = 500_000;
      final int recordSize = 102400;

      long begin = System.currentTimeMillis();
      for (int i = 0; i < totLogEntries; i++) {
        OPhase1Tx item =
            new OPhase1Tx() {
              @Override
              public void serialize(DataOutput output) throws IOException {
                output.write(new byte[recordSize]);
              }
            };
        log.log(item);
      }
      System.out.println("Elapsed (ms): " + (System.currentTimeMillis() - begin));
      System.out.println(
          "Entries per record: " + totLogEntries / ((System.currentTimeMillis() - begin) / 1000));
      System.out.println(
          "Kb/s: " + recordSize * totLogEntries / (System.currentTimeMillis() - begin));

    } finally {
      for (File file1 : file.toFile().listFiles()) {
        file1.delete();
      }
      file.toFile().delete();
    }
  }

  @Test
  @Ignore
  public void stressTestMt() throws IOException {

    Path file = Files.createTempDirectory(".");
    OPersistentOperationalLogV1 log =
        new OPersistentOperationalLogV1(file.toString(), (id) -> new OPhase1Tx());
    log.setLeader(true, 0);
    try {
      final long totLogEntries = 1_000;
      final int recordSize = 1024;
      final int nThreads = 8;
      final List<Thread> threads = new ArrayList<>();

      long begin = System.currentTimeMillis();

      for (int iThread = 0; iThread < nThreads; iThread++) {

        Thread thread =
            new Thread() {
              @Override
              public void run() {

                for (int i = 0; i < totLogEntries; i++) {
                  OPhase1Tx item =
                      new OPhase1Tx() {
                        @Override
                        public void serialize(DataOutput output) throws IOException {
                          output.write(new byte[(int) (recordSize * 2 * Math.random())]);
                        }
                      };
                  log.log(item);
                  if (i % 100 == 0) {
                    System.out.println("flushed " + i);
                  }
                }
              }
            };
        threads.add(thread);
        thread.start();
      }

      threads.forEach(
          x -> {
            try {
              x.join();
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          });

      System.out.println("Elapsed (ms): " + (System.currentTimeMillis() - begin));
      System.out.println(
          "Entries per second: "
              + totLogEntries * nThreads / ((System.currentTimeMillis() - begin) / 1000));
      System.out.println(
          "Kb/s: " + totLogEntries * recordSize * nThreads / (System.currentTimeMillis() - begin));

    } finally {
      for (File file1 : file.toFile().listFiles()) {
        file1.delete();
      }
      file.toFile().delete();
    }
  }

  @Test
  public void testLogReceived() throws IOException {
    Path file = Files.createTempDirectory(".");
    OPersistentOperationalLogV1 log =
        new OPersistentOperationalLogV1(file.toString(), (id) -> new OPhase1Tx());
    log.setLeader(false, 0);

    Assert.assertTrue(log.logReceived(new OLogId(0, 0, 0), new OPhase1Tx()));
    Assert.assertFalse(log.logReceived(new OLogId(0, 0, 0), new OPhase1Tx()));
    Assert.assertTrue(log.logReceived(new OLogId(1, 0, 0), new OPhase1Tx()));
    Assert.assertTrue(log.logReceived(new OLogId(1, 1, 0), new OPhase1Tx()));
    Assert.assertTrue(log.logReceived(new OLogId(2, 1, 1), new OPhase1Tx()));
    Assert.assertFalse(log.logReceived(new OLogId(3, 1, 0), new OPhase1Tx()));
    Assert.assertTrue(log.logReceived(new OLogId(3, 1, 1), new OPhase1Tx()));
  }

  @Test
  public void testStartWithEmptyLog() throws IOException {
    Path file = Files.createTempDirectory(".");
    OPersistentOperationalLogV1 log =
        new OPersistentOperationalLogV1(file.toString(), (id) -> new OPhase1Tx());
    log.setLeader(false, 0);

    Assert.assertFalse(log.logReceived(new OLogId(100, 4, 0), new OPhase1Tx()));
  }

  @Test
  public void testLog() throws IOException {
    Path file = Files.createTempDirectory(".");
    OPersistentOperationalLogV1 log =
        new OPersistentOperationalLogV1(file.toString(), (id) -> new OPhase1Tx());
    log.setLeader(true, 0);

    OLogId logId = log.log(new OPhase1Tx());
    Assert.assertEquals(0L, logId.getId());
    Assert.assertEquals(0L, logId.getTerm());

    logId = log.log(new OPhase1Tx());
    Assert.assertEquals(1L, logId.getId());
    Assert.assertEquals(0L, logId.getTerm());
    Assert.assertEquals(0L, logId.getPreviousIdTerm());

    log.setLeader(true, 1);

    logId = log.log(new OPhase1Tx());
    Assert.assertEquals(2L, logId.getId());
    Assert.assertEquals(1L, logId.getTerm());
    Assert.assertEquals(0L, logId.getPreviousIdTerm());

    logId = log.log(new OPhase1Tx());
    Assert.assertEquals(3L, logId.getId());
    Assert.assertEquals(1L, logId.getTerm());
    Assert.assertEquals(1L, logId.getPreviousIdTerm());

    log.setLeader(false, 2);

    Assert.assertFalse(log.logReceived(new OLogId(4, 2, 0), new OPhase1Tx()));
    Assert.assertTrue(log.logReceived(new OLogId(4, 2, 1), new OPhase1Tx()));

    log.setLeader(true, 3);

    logId = log.log(new OPhase1Tx());
    Assert.assertEquals(5L, logId.getId());
    Assert.assertEquals(3L, logId.getTerm());
    Assert.assertEquals(2L, logId.getPreviousIdTerm());

    log.log(new OPhase1Tx()); // 6
    log.log(new OPhase1Tx()); // 7

    log.setLeader(false, 4);
    Assert.assertTrue(log.logReceived(new OLogId(6, 4, 3), new OPhase1Tx()));

    OLogId last = log.lastPersistentLog();
    Assert.assertEquals(6L, last.getId());
    Assert.assertEquals(4L, last.getTerm());
    Assert.assertEquals(3L, last.getPreviousIdTerm());
  }

  @Test
  public void testRecover1() throws IOException {
    Path file = Files.createTempDirectory(".");
    OPersistentOperationalLogV1 log =
        new OPersistentOperationalLogV1(file.toString(), (id) -> new OPhase1Tx());
    log.setLeader(true, 0);

    log.log(new OPhase1Tx());
    log.log(new OPhase1Tx());
    log.log(new OPhase1Tx());

    OLogId lastLogId = log.log(new OPhase1Tx());

    log.close();

    log = new OPersistentOperationalLogV1(file.toString(), (id) -> new OPhase1Tx());
    log.setLeader(true, 0);
    Assert.assertEquals(lastLogId, log.lastPersistentLog());
  }

  @Test
  public void testRecover2() throws IOException {
    Path file = Files.createTempDirectory(".");
    OPersistentOperationalLogV1 log =
        new OPersistentOperationalLogV1(file.toString(), (id) -> new OPhase1Tx());
    log.setLeader(true, 0);

    log.log(new OPhase1Tx());
    log.log(new OPhase1Tx());
    log.log(new OPhase1Tx());

    OLogId lastLogId = log.log(new OPhase1Tx());

    log.close();

    File logFile = new File(log.calculateLogFileFullPath(0));

    Assert.assertTrue(logFile.exists());
    FileWriter writer = new FileWriter(logFile, true);
    writer.write("foobar");
    writer.flush();
    writer.close();

    log = new OPersistentOperationalLogV1(file.toString(), (id) -> new OPhase1Tx());
    log.setLeader(true, 0);
    Assert.assertEquals(lastLogId, log.lastPersistentLog());
  }

  @Test
  public void testSearchFrom() throws IOException {
    Path file = Files.createTempDirectory(".");
    OPersistentOperationalLogV1 master =
        new OPersistentOperationalLogV1(file.toString(), (id) -> new OPhase1Tx());
    master.setLeader(true, 0);
    master.log(new OPhase1Tx());
    master.log(new OPhase1Tx());
    master.log(new OPhase1Tx());
    master.log(new OPhase1Tx()); // 4
    master.setLeader(true, 1);
    master.log(new OPhase1Tx());
    master.log(new OPhase1Tx());
    master.log(new OPhase1Tx());
    master.log(new OPhase1Tx());
    master.log(new OPhase1Tx());
    master.log(new OPhase1Tx());
    master.log(new OPhase1Tx());
    master.log(new OPhase1Tx());
    master.log(new OPhase1Tx());
    master.log(new OPhase1Tx());
    master.log(new OPhase1Tx());
    master.log(new OPhase1Tx());
    master.setLeader(true, 2);
    master.log(new OPhase1Tx());
    master.log(new OPhase1Tx());
    master.log(new OPhase1Tx());
    master.log(new OPhase1Tx());
    master.log(new OPhase1Tx());
    master.log(new OPhase1Tx());
    master.log(new OPhase1Tx());
    master.log(new OPhase1Tx());
    master.log(new OPhase1Tx());
    master.log(new OPhase1Tx());
    master.log(new OPhase1Tx());
    master.log(new OPhase1Tx());

    Path file2 = Files.createTempDirectory(".");
    OPersistentOperationalLogV1 follower =
        new OPersistentOperationalLogV1(file2.toString(), (id) -> new OPhase1Tx());
    follower.setLeader(true, 0);
    follower.log(new OPhase1Tx());
    follower.log(new OPhase1Tx());
    follower.log(new OPhase1Tx());
    follower.log(new OPhase1Tx());
    follower.log(new OPhase1Tx());
    follower.log(new OPhase1Tx());
    follower.log(new OPhase1Tx());
    follower.log(new OPhase1Tx());
    follower.log(new OPhase1Tx());
    follower.log(new OPhase1Tx());

    OOplogIterator data = master.searchFrom(follower.lastPersistentLog()).get();

    follower.setLeader(false, 1);
    while (data.hasNext()) {
      OOperationLogEntry item = data.next();
      boolean result = follower.logReceived(item.getLogId(), item.getRequest());
    }
    data.close();

    OOplogIterator masterIterator = master.iterate(0, 100);
    OOplogIterator followerIterator = follower.iterate(0, 100);
    while (masterIterator.hasNext()) {
      Assert.assertTrue(followerIterator.hasNext());
      Assert.assertEquals(masterIterator.next().getLogId(), followerIterator.next().getLogId());
    }
    Assert.assertFalse(followerIterator.hasNext());
    masterIterator.close();
    followerIterator.close();
    master.close();
    follower.close();
  }

  @Test
  public void testSearchFromEmpty() throws IOException {
    Path file = Files.createTempDirectory(".");
    OPersistentOperationalLogV1 master =
        new OPersistentOperationalLogV1(file.toString(), (id) -> new OPhase1Tx());
    master.setLeader(true, 0);

    Assert.assertFalse(master.searchFrom(new OLogId(0, 10, 0)).isPresent());
    master.close();
  }
}
