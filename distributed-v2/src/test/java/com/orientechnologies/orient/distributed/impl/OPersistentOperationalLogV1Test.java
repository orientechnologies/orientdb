package com.orientechnologies.orient.distributed.impl;

import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;
import com.orientechnologies.orient.distributed.impl.coordinator.OOperationLogEntry;
import com.orientechnologies.orient.distributed.impl.coordinator.mocktx.OPhase1Tx;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class OPersistentOperationalLogV1Test {

  @Test
  public void testReadLastId() throws IOException {
    OCoordinateMessagesFactory factory = new OCoordinateMessagesFactory();
    Path file = Files.createTempDirectory(".");
    try {
      OPersistentOperationalLogV1 oplog = new OPersistentOperationalLogV1(file.toString(),
              (id) -> factory.createOperationRequest(id));
      OLogId logId = null;
      for (int i = 0; i < 10; i++) {
        OPhase1Tx req = new OPhase1Tx();
        logId = oplog.log(req);
      }
      oplog.close();
      oplog = new OPersistentOperationalLogV1(file.toString(), (id) -> factory.createOperationRequest(id));
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
    OPersistentOperationalLogV1 log = new OPersistentOperationalLogV1(file.toString(), (id) -> new OPhase1Tx());

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
    OPersistentOperationalLogV1 log = new OPersistentOperationalLogV1(file.toString(), (id) -> new OPhase1Tx());

    try {
      int totLogEntries = 50_000;
      for (int i = 0; i < totLogEntries; i++) {
        OPhase1Tx item = new OPhase1Tx();
        log.log(item);
      }

      int nextEntry = 10_000;
      Iterator<OOperationLogEntry> iteartor = log.iterate(new OLogId(nextEntry), new OLogId(totLogEntries - 1));
      while (nextEntry < totLogEntries) {
        OOperationLogEntry item = iteartor.next();
        Assert.assertEquals(nextEntry++, item.getLogId().getId());
      }
      Assert.assertFalse(iteartor.hasNext());

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
    OPersistentOperationalLogV1 log = new OPersistentOperationalLogV1(file.toString(), (id) -> new OPhase1Tx());

    try {
      int totLogEntries = 50_000;

      for (int i = 0; i < totLogEntries; i++) {
        OPhase1Tx item = new OPhase1Tx();
        log.log(item);
      }

      Iterator<OOperationLogEntry> iteartor = log.iterate(new OLogId(10_000), new OLogId(40_000 - 1));
      for (int i = 10_000; i < 40_000; i++) {
        OOperationLogEntry item = iteartor.next();
        Assert.assertEquals(i, item.getLogId().getId());
      }
      Assert.assertFalse(iteartor.hasNext());

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
    OPersistentOperationalLogV1 log = new OPersistentOperationalLogV1(file.toString(), (id) -> new OPhase1Tx());

    try {
      final long totLogEntries = 500_000;
      final int recordSize = 102400;

      long begin = System.currentTimeMillis();
      for (int i = 0; i < totLogEntries; i++) {
        OPhase1Tx item = new OPhase1Tx() {
          @Override
          public void serialize(DataOutput output) throws IOException {
            output.write(new byte[recordSize]);
          }
        };
        log.log(item);
      }
      System.out.println("Elapsed (ms): " + (System.currentTimeMillis() - begin));
      System.out.println("Entries per record: " + totLogEntries / ((System.currentTimeMillis() - begin) / 1000));
      System.out.println("Kb/s: " + recordSize * totLogEntries / (System.currentTimeMillis() - begin));

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
    OPersistentOperationalLogV1 log = new OPersistentOperationalLogV1(file.toString(), (id) -> new OPhase1Tx());

    try {
      final long totLogEntries = 100_000;
      final int recordSize = 102400;
      final int nThreads = 5;
      final List<Thread> threads = new ArrayList<>();

      long begin = System.currentTimeMillis();

      for (int iThread = 0; iThread < nThreads; iThread++) {

        Thread thread = new Thread() {
          @Override
          public void run() {

            for (int i = 0; i < totLogEntries; i++) {
              OPhase1Tx item = new OPhase1Tx() {
                @Override
                public void serialize(DataOutput output) throws IOException {
                  output.write(new byte[recordSize]);
                }
              };
              log.log(item);
            }
          }
        };
        threads.add(thread);
        thread.start();
      }

      threads.forEach(x -> {
        try {
          x.join();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      });


      System.out.println("Elapsed (ms): " + (System.currentTimeMillis() - begin));
      System.out.println("Entries per second: " + totLogEntries * nThreads / ((System.currentTimeMillis() - begin) / 1000));
      System.out.println("Kb/s: " + totLogEntries * recordSize * nThreads / (System.currentTimeMillis() - begin));

    } finally {
      for (File file1 : file.toFile().listFiles()) {
        file1.delete();
      }
      file.toFile().delete();
    }
  }
}

