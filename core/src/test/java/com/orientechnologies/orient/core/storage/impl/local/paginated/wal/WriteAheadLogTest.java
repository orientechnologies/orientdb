package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;

/**
 * @author Andrey Lomakin
 * @since 29.04.13
 */
@Test
public class WriteAheadLogTest {
  private OWriteAheadLog writeAheadLog;
  private File           testDir;

  @BeforeClass
  public void beforeClass() {
    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null || buildDirectory.isEmpty())
      buildDirectory = ".";

    testDir = new File(buildDirectory, "writeAheadLogTest");
    if (!testDir.exists())
      testDir.mkdir();
  }

  @BeforeMethod
  public void beforeMethod() throws Exception {
    writeAheadLog = createWAL();
  }

  private OWriteAheadLog createWAL() throws IOException {
    return new OWriteAheadLog(1024, 1000, 2048, 100L * 1024L * 1024L * 1024L, "WriteAheadLogTest", testDir.getAbsolutePath());
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    writeAheadLog.delete();
  }

  @AfterClass
  public void afterClass() {
    File testDir = new File("writeAheadLogTest");
    if (testDir.exists())
      testDir.delete();
  }

  public void testPageIsBroken() throws Exception {
    writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 1, 2, 3 }, 10, 20, "test"));
    writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 2, 2, 3 }, 10, 20, "test"));

    OLogSequenceNumber numberToDelete = writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 3, 2, 3 }, 10, 20, "test"));

    writeAheadLog.logCheckPointStart();
    Assert.assertNotNull(writeAheadLog.getLastCheckpoint());

    long logSize = writeAheadLog.size();
    writeAheadLog.close();

    RandomAccessFile walFile = new RandomAccessFile(new File(writeAheadLog.getWalLocation(), "WriteAheadLogTest.0.wal"), "rw");
    Assert.assertEquals(logSize, walFile.length());

    walFile.seek(numberToDelete.getPosition() + 2 * OIntegerSerializer.INT_SIZE + 1);

    int bt = walFile.read();
    bt++;

    walFile.seek(numberToDelete.getPosition() + 2 * OIntegerSerializer.INT_SIZE + 1);
    walFile.write(bt);
    walFile.close();

    writeAheadLog = createWAL();

    OWALRecord walRecord = writeAheadLog.read(OLogSequenceNumber.BEGIN);
    walRecord = writeAheadLog.readNext(walRecord.getLsn());

    try {
      writeAheadLog.readNext(walRecord.getLsn());
      Assert.fail();
    } catch (OWriteAheadLogRecordIsBrokenException e) {
    }

    writeAheadLog.restore();

    walRecord = writeAheadLog.read(OLogSequenceNumber.BEGIN);
    walRecord = writeAheadLog.readNext(walRecord.getLsn());
    Assert.assertNull(writeAheadLog.readNext(walRecord.getLsn()));

    Assert.assertNull(writeAheadLog.getLastCheckpoint());
    Assert.assertEquals(writeAheadLog.size(), new File(writeAheadLog.getWalLocation(), "WriteAheadLogTest.0.wal").length());

    writeAheadLog.close();
    writeAheadLog = createWAL();
    Assert.assertNull(writeAheadLog.getLastCheckpoint());
  }

  public void testPageIsBrokenWithSecondMasterRecord() throws Exception {
    writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 1, 2, 3 }, 10, 20, "test"));

    OLogSequenceNumber firstCheckPoint = writeAheadLog.logCheckPointStart();

    OLogSequenceNumber numberToDelete = writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 3, 2, 3 }, 10, 20, "test"));

    OLogSequenceNumber secondCheckPoint = writeAheadLog.logCheckPointStart();

    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), secondCheckPoint);

    long logSize = writeAheadLog.size();
    writeAheadLog.close();

    RandomAccessFile walFile = new RandomAccessFile(new File(writeAheadLog.getWalLocation(), "WriteAheadLogTest.0.wal"), "rw");
    Assert.assertEquals(logSize, walFile.length());

    walFile.seek(numberToDelete.getPosition() + 2 * OIntegerSerializer.INT_SIZE + 1);

    int bt = walFile.read();
    bt++;

    walFile.seek(numberToDelete.getPosition() + 2 * OIntegerSerializer.INT_SIZE + 1);
    walFile.write(bt);
    walFile.close();

    writeAheadLog = createWAL();

    OWALRecord walRecord = writeAheadLog.read(OLogSequenceNumber.BEGIN);
    walRecord = writeAheadLog.readNext(walRecord.getLsn());

    try {
      writeAheadLog.readNext(walRecord.getLsn());
      Assert.fail();
    } catch (OWriteAheadLogRecordIsBrokenException e) {
    }

    writeAheadLog.restore();

    walRecord = writeAheadLog.read(OLogSequenceNumber.BEGIN);
    walRecord = writeAheadLog.readNext(walRecord.getLsn());
    Assert.assertNull(writeAheadLog.readNext(walRecord.getLsn()));

    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), firstCheckPoint);
    Assert.assertEquals(writeAheadLog.size(), new File(writeAheadLog.getWalLocation(), "WriteAheadLogTest.0.wal").length());
    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), firstCheckPoint);
  }

  public void testPageIsBrokenWithFirstMasterRecord() throws Exception {
    writeAheadLog.logCheckPointStart();

    OLogSequenceNumber firstCheckPoint = writeAheadLog.logCheckPointStart();

    OLogSequenceNumber numberToDelete = writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 3, 2, 3 }, 10, 20, "test"));

    OLogSequenceNumber secondCheckPoint = writeAheadLog.logCheckPointStart();

    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), secondCheckPoint);

    long logSize = writeAheadLog.size();
    writeAheadLog.close();

    RandomAccessFile walFile = new RandomAccessFile(new File(writeAheadLog.getWalLocation(), "WriteAheadLogTest.0.wal"), "rw");
    Assert.assertEquals(logSize, walFile.length());

    walFile.seek(numberToDelete.getPosition() + 2 * OIntegerSerializer.INT_SIZE + 1);

    int bt = walFile.read();
    bt++;

    walFile.seek(numberToDelete.getPosition() + 2 * OIntegerSerializer.INT_SIZE + 1);
    walFile.write(bt);
    walFile.close();

    writeAheadLog = createWAL();

    OWALRecord walRecord = writeAheadLog.read(OLogSequenceNumber.BEGIN);
    walRecord = writeAheadLog.readNext(walRecord.getLsn());

    try {
      writeAheadLog.readNext(walRecord.getLsn());
      Assert.fail();
    } catch (OWriteAheadLogRecordIsBrokenException e) {
    }

    writeAheadLog.restore();

    walRecord = writeAheadLog.read(OLogSequenceNumber.BEGIN);
    walRecord = writeAheadLog.readNext(walRecord.getLsn());
    Assert.assertNull(writeAheadLog.readNext(walRecord.getLsn()));

    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), firstCheckPoint);
    Assert.assertEquals(writeAheadLog.size(), new File(writeAheadLog.getWalLocation(), "WriteAheadLogTest.0.wal").length());

    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), firstCheckPoint);
  }

  public void testPageIsBrokenWithBothMasterRecords() throws Exception {
    writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 1, 2, 3 }, 10, 20, "test"));
    writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 1, 2, 3 }, 10, 20, "test"));

    OLogSequenceNumber numberToDelete = writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 1, 2, 3 }, 10, 20, "test"));

    writeAheadLog.logCheckPointStart();
    writeAheadLog.logCheckPointStart();

    Assert.assertNotNull(writeAheadLog.getLastCheckpoint());

    long logSize = writeAheadLog.size();
    writeAheadLog.close();

    RandomAccessFile walFile = new RandomAccessFile(new File(writeAheadLog.getWalLocation(), "WriteAheadLogTest.0.wal"), "rw");
    Assert.assertEquals(logSize, walFile.length());

    walFile.seek(numberToDelete.getPosition() + 2 * OIntegerSerializer.INT_SIZE + 1);

    int bt = walFile.read();
    bt++;

    walFile.seek(numberToDelete.getPosition() + 2 * OIntegerSerializer.INT_SIZE + 1);
    walFile.write(bt);
    walFile.close();

    writeAheadLog = createWAL();

    OWALRecord walRecord = writeAheadLog.read(OLogSequenceNumber.BEGIN);
    walRecord = writeAheadLog.readNext(walRecord.getLsn());

    try {
      writeAheadLog.readNext(walRecord.getLsn());
      Assert.fail();
    } catch (OWriteAheadLogRecordIsBrokenException e) {
    }

    writeAheadLog.restore();

    walRecord = writeAheadLog.read(OLogSequenceNumber.BEGIN);
    walRecord = writeAheadLog.readNext(walRecord.getLsn());
    Assert.assertNull(writeAheadLog.readNext(walRecord.getLsn()));

    Assert.assertNull(writeAheadLog.getLastCheckpoint());
    Assert.assertEquals(writeAheadLog.size(), new File(writeAheadLog.getWalLocation(), "WriteAheadLogTest.0.wal").length());

    writeAheadLog.close();
    writeAheadLog = createWAL();

    Assert.assertNull(writeAheadLog.getLastCheckpoint());
  }

  public void testPageIsNotWrittenFully() throws Exception {
    writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 1, 2, 3 }, 10, 20, "test"));
    writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 2, 2, 3 }, 10, 20, "test"));
    writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 3, 2, 3 }, 10, 20, "test"));
    writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 4, 2, 3 }, 10, 20, "test"));

    long logSize = writeAheadLog.size();
    writeAheadLog.close();

    RandomAccessFile walFile = new RandomAccessFile(new File(writeAheadLog.getWalLocation(), "WriteAheadLogTest.0.wal"), "rw");
    Assert.assertEquals(logSize, walFile.length());

    walFile.setLength(walFile.length() - 2);
    walFile.close();

    writeAheadLog = createWAL();

    OWALRecord walRecord = writeAheadLog.read(OLogSequenceNumber.BEGIN);
    walRecord = writeAheadLog.readNext(walRecord.getLsn());
    walRecord = writeAheadLog.readNext(walRecord.getLsn());

    try {
      writeAheadLog.readNext(walRecord.getLsn());
      Assert.fail();
    } catch (OWriteAheadLogRecordIsBrokenException e) {
    }

    writeAheadLog.restore();

    walRecord = writeAheadLog.read(OLogSequenceNumber.BEGIN);
    walRecord = writeAheadLog.readNext(walRecord.getLsn());
    walRecord = writeAheadLog.readNext(walRecord.getLsn());

    Assert.assertNull(writeAheadLog.readNext(walRecord.getLsn()));
    Assert.assertEquals(writeAheadLog.size(), new File(writeAheadLog.getWalLocation(), "WriteAheadLogTest.0.wal").length());
  }

  public void testWriteSingleRecord() throws Exception {

    writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 1, 2, 3 }, 10, 20, "test"));

    OWALRecord walRecord = writeAheadLog.read(OLogSequenceNumber.BEGIN);
    Assert.assertTrue(walRecord instanceof OSetPageDataRecord);

    OSetPageDataRecord setPageDataRecord = (OSetPageDataRecord) walRecord;
    Assert.assertEquals(setPageDataRecord.getPageOffset(), 10);
    Assert.assertEquals(setPageDataRecord.getData(), new byte[] { 1, 2, 3 });
    Assert.assertEquals(setPageDataRecord.getPageIndex(), 20);
    Assert.assertEquals(setPageDataRecord.getFileName(), "test");

    Assert.assertNull(writeAheadLog.readNext(OLogSequenceNumber.BEGIN));

    writeAheadLog.close();
    writeAheadLog = createWAL();

    walRecord = writeAheadLog.read(OLogSequenceNumber.BEGIN);
    Assert.assertEquals(walRecord.getLsn(), OLogSequenceNumber.BEGIN);
    Assert.assertTrue(walRecord instanceof OSetPageDataRecord);

    setPageDataRecord = (OSetPageDataRecord) walRecord;
    Assert.assertEquals(setPageDataRecord.getPageOffset(), 10);
    Assert.assertEquals(setPageDataRecord.getData(), new byte[] { 1, 2, 3 });
    Assert.assertEquals(setPageDataRecord.getPageIndex(), 20);
    Assert.assertEquals(setPageDataRecord.getFileName(), "test");

    Assert.assertNull(writeAheadLog.readNext(OLogSequenceNumber.BEGIN));
  }

  public void testFirstMasterRecordUpdate() throws Exception {
    writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 1, 2, 3 }, 10, 20, "test"));
    OLogSequenceNumber masterLSN = writeAheadLog.logCheckPointStart();

    writeAheadLog.logCheckPointEnd();

    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), masterLSN);
    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), masterLSN);
  }

  public void testSecondMasterRecordUpdate() throws Exception {
    writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 1, 2, 3 }, 10, 20, "test"));

    writeAheadLog.logCheckPointStart();
    writeAheadLog.logCheckPointEnd();

    writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 1, 2, 3 }, 10, 20, "test"));

    OLogSequenceNumber checkpointLSN = writeAheadLog.logCheckPointStart();
    writeAheadLog.logCheckPointEnd();

    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), checkpointLSN);
    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), checkpointLSN);
  }

  public void testThirdMasterRecordUpdate() throws Exception {
    writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 1, 2, 3 }, 10, 20, "test"));

    writeAheadLog.logCheckPointStart();
    writeAheadLog.logCheckPointEnd();

    writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 1, 2, 3 }, 10, 20, "test"));

    writeAheadLog.logCheckPointStart();
    writeAheadLog.logCheckPointEnd();

    writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 1, 2, 3 }, 10, 20, "test"));

    OLogSequenceNumber checkpointLSN = writeAheadLog.logCheckPointStart();
    writeAheadLog.logCheckPointEnd();

    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), checkpointLSN);
    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), checkpointLSN);
  }

  public void testFirstMasterRecordIsBrokenSingleRecord() throws Exception {
    writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 1, 2, 3 }, 10, 20, "test"));

    writeAheadLog.logCheckPointStart();
    writeAheadLog.logCheckPointEnd();

    writeAheadLog.close();

    RandomAccessFile mrFile = new RandomAccessFile(new File(writeAheadLog.getWalLocation(), "WriteAheadLogTest.wmr"), "rw");
    mrFile.seek(OIntegerSerializer.INT_SIZE + 1);

    int bt = mrFile.read();
    mrFile.seek(OIntegerSerializer.INT_SIZE + 1);
    mrFile.write(bt + 1);
    mrFile.close();

    writeAheadLog = createWAL();
    Assert.assertNull(writeAheadLog.getLastCheckpoint());
  }

  public void testSecondMasterRecordIsBroken() throws Exception {
    writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 1, 2, 3 }, 10, 20, "test"));

    OLogSequenceNumber checkPointLSN = writeAheadLog.logCheckPointStart();
    writeAheadLog.logCheckPointEnd();

    writeAheadLog.logCheckPointStart();
    writeAheadLog.logCheckPointEnd();

    writeAheadLog.close();

    RandomAccessFile mrFile = new RandomAccessFile(new File(writeAheadLog.getWalLocation(), "WriteAheadLogTest.wmr"), "rw");
    mrFile.seek(3 * OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE);

    int bt = mrFile.read();
    mrFile.seek(3 * OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE);
    mrFile.write(bt + 1);
    mrFile.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), checkPointLSN);
  }

  public void testFirstMasterRecordIsBrokenThreeCheckpoints() throws Exception {
    writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 1, 2, 3 }, 10, 20, "test"));

    writeAheadLog.logCheckPointStart();
    writeAheadLog.logCheckPointEnd();

    OLogSequenceNumber checkPointLSN = writeAheadLog.logCheckPointStart();
    writeAheadLog.logCheckPointEnd();

    writeAheadLog.logCheckPointStart();
    writeAheadLog.logCheckPointEnd();

    writeAheadLog.close();

    RandomAccessFile mrFile = new RandomAccessFile(new File(writeAheadLog.getWalLocation(), "WriteAheadLogTest.wmr"), "rw");
    mrFile.seek(0);

    int bt = mrFile.read();
    mrFile.seek(0);
    mrFile.write(bt + 1);
    mrFile.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), checkPointLSN);
  }

  public void testWriteMultipleRecords() throws Exception {
    List<OSetPageDataRecord> writtenRecords = new ArrayList<OSetPageDataRecord>();
    Random rnd = new Random();

    final int recordsToWrite = 1000;
    for (int i = 0; i < recordsToWrite; i++) {
      byte[] data = new byte[10];
      rnd.nextBytes(data);

      int pageOffset = rnd.nextInt(65536);
      long pageIndex = rnd.nextLong();
      OSetPageDataRecord setPageDataRecord = new OSetPageDataRecord(data, pageOffset, pageIndex, "test");
      writtenRecords.add(setPageDataRecord);

      writeAheadLog.logRecord(setPageDataRecord);
    }

    assertLogContent(writeAheadLog, writtenRecords);
    assertLogContent(writeAheadLog, writtenRecords.subList(writtenRecords.size() / 2, writtenRecords.size()));

    writeAheadLog.close();

    writeAheadLog = createWAL();
    assertLogContent(writeAheadLog, writtenRecords);
    assertLogContent(writeAheadLog, writtenRecords.subList(writtenRecords.size() / 2, writtenRecords.size()));
  }

  public void testAppendMultipleRecordsAfterClose() throws Exception {
    List<OSetPageDataRecord> writtenRecords = new ArrayList<OSetPageDataRecord>();
    Random rnd = new Random();

    final int recordsToWrite = 1000;
    for (int i = 0; i < recordsToWrite; i++) {
      byte[] data = new byte[10];
      rnd.nextBytes(data);

      int pageOffset = rnd.nextInt(65536);
      long pageIndex = rnd.nextLong();
      OSetPageDataRecord setPageDataRecord = new OSetPageDataRecord(data, pageOffset, pageIndex, "test");
      writtenRecords.add(setPageDataRecord);

      writeAheadLog.logRecord(setPageDataRecord);
    }

    writeAheadLog.close();
    writeAheadLog = createWAL();

    for (int i = 0; i < recordsToWrite; i++) {
      byte[] data = new byte[10];
      rnd.nextBytes(data);

      int pageOffset = rnd.nextInt(65536);
      long pageIndex = rnd.nextLong();
      OSetPageDataRecord setPageDataRecord = new OSetPageDataRecord(data, pageOffset, pageIndex, "test");
      writtenRecords.add(setPageDataRecord);

      writeAheadLog.logRecord(setPageDataRecord);
    }

    assertLogContent(writeAheadLog, writtenRecords);
    assertLogContent(writeAheadLog, writtenRecords.subList(writtenRecords.size() / 2, writtenRecords.size()));

    writeAheadLog.close();

    writeAheadLog = createWAL();
    assertLogContent(writeAheadLog, writtenRecords);
    assertLogContent(writeAheadLog, writtenRecords.subList(writtenRecords.size() / 2, writtenRecords.size()));
  }

  private void assertLogContent(OWriteAheadLog writeAheadLog, List<OSetPageDataRecord> writtenRecords) throws Exception {
    Iterator<OSetPageDataRecord> iterator = writtenRecords.iterator();

    OWALRecord writtenRecord = iterator.next();
    OWALRecord readRecord = writeAheadLog.read(writtenRecord.getLsn());

    Assert.assertEquals(writtenRecord, readRecord);
    while (iterator.hasNext()) {
      readRecord = writeAheadLog.readNext(readRecord.getLsn());
      writtenRecord = iterator.next();

      Assert.assertEquals(writtenRecord, readRecord);
    }

    Assert.assertNull(writeAheadLog.readNext(readRecord.getLsn()));
  }
}
