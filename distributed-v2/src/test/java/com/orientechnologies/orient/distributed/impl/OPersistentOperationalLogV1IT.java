package com.orientechnologies.orient.distributed.impl;

import com.orientechnologies.orient.distributed.impl.coordinator.OCoordinateMessagesFactory;
import com.orientechnologies.orient.distributed.impl.coordinator.OLogId;
import com.orientechnologies.orient.distributed.impl.coordinator.OOperationLog;
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

public class OPersistentOperationalLogV1IT {


  @Test
  public void testIterate() throws IOException {

    Path file = Files.createTempDirectory(".");
    OPersistentOperationalLogV1 log = new OPersistentOperationalLogV1(file.toString(), (id) -> new OPhase1Tx());
    log.setLeader(true, 0);
    try {
      int totLogEntries = 50_000;
      for (int i = 0; i < totLogEntries; i++) {
        OPhase1Tx item = new OPhase1Tx();
        log.log(item);
      }

      long nextEntry = 10_000;
      Iterator<OOperationLogEntry> iteartor = log.iterate(nextEntry, totLogEntries - 1);
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
    log.setLeader(true, 0);
    try {
      int totLogEntries = 50_000;

      for (int i = 0; i < totLogEntries; i++) {
        OPhase1Tx item = new OPhase1Tx();
        log.log(item);
      }

      Iterator<OOperationLogEntry> iteartor = log.iterate(10_000, 40_000 - 1);
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


  /**
   * test off-by-one errors across oplog file split
   *
   * @throws IOException
   */
  @Test
  public void testRemoveAfter() throws IOException {
    for (int iter = -2; iter < 3; iter++) {


      Path file = Files.createTempDirectory(".");
      OPersistentOperationalLogV1 log = new OPersistentOperationalLogV1(file.toString(), (id) -> new OPhase1Tx());
      log.setLeader(true, 0);

      try {
        int totLogEntries = OPersistentOperationalLogV1.LOG_ENTRIES_PER_FILE + 100;
        int cutTo = OPersistentOperationalLogV1.LOG_ENTRIES_PER_FILE + iter;

        for (int i = 0; i < totLogEntries; i++) {
          OPhase1Tx item = new OPhase1Tx();
          log.log(item);
        }
        OOperationLog.LogIdStatus status = log.removeAfter(new OLogId(cutTo, 0, 0));

        Assert.assertEquals(OOperationLog.LogIdStatus.PRESENT, status);

        Iterator<OOperationLogEntry> iteartor = log.iterate(0, totLogEntries);
        for (int i = 0; i <= cutTo; i++) {
          OOperationLogEntry item = iteartor.next();
          Assert.assertEquals(i, item.getLogId().getId());
        }
        try {
          Assert.assertFalse("Failed iteration " + iter, iteartor.hasNext());
        } catch (Exception e) {
          System.out.println("Failed iteration " + iter);
          throw e;
        }

      } finally {
        for (File file1 : file.toFile().listFiles()) {
          file1.delete();
        }
        file.toFile().delete();
      }
    }
  }

}

