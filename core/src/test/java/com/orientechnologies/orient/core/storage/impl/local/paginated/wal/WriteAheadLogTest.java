package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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
    OLocalPaginatedStorage paginatedStorage = mock(OLocalPaginatedStorage.class);
    when(paginatedStorage.getName()).thenReturn("WriteAheadLogTest");
    when(paginatedStorage.getStoragePath()).thenReturn(testDir.getAbsolutePath());

    return new OWriteAheadLog(1024, -1, 2048, 100L * 1024L * 1024L * 1024L, paginatedStorage);
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    if (writeAheadLog != null)
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

    writeAheadLog.logFuzzyCheckPointStart();
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

    OLogSequenceNumber firstCheckPoint = writeAheadLog.logFuzzyCheckPointStart();

    OLogSequenceNumber numberToDelete = writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 3, 2, 3 }, 10, 20, "test"));

    OLogSequenceNumber secondCheckPoint = writeAheadLog.logFuzzyCheckPointStart();

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
    writeAheadLog.logFuzzyCheckPointStart();

    OLogSequenceNumber firstCheckPoint = writeAheadLog.logFuzzyCheckPointStart();

    OLogSequenceNumber numberToDelete = writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 3, 2, 3 }, 10, 20, "test"));

    OLogSequenceNumber secondCheckPoint = writeAheadLog.logFuzzyCheckPointStart();

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

    writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointStart();

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
    OLogSequenceNumber masterLSN = writeAheadLog.logFuzzyCheckPointStart();

    writeAheadLog.logFuzzyCheckPointEnd();

    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), masterLSN);
    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), masterLSN);
  }

  public void testSecondMasterRecordUpdate() throws Exception {
    writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 1, 2, 3 }, 10, 20, "test"));

    writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 1, 2, 3 }, 10, 20, "test"));

    OLogSequenceNumber checkpointLSN = writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointEnd();

    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), checkpointLSN);
    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), checkpointLSN);
  }

  public void testThirdMasterRecordUpdate() throws Exception {
    writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 1, 2, 3 }, 10, 20, "test"));

    writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 1, 2, 3 }, 10, 20, "test"));

    writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 1, 2, 3 }, 10, 20, "test"));

    OLogSequenceNumber checkpointLSN = writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointEnd();

    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), checkpointLSN);
    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), checkpointLSN);
  }

  public void testFirstMasterRecordIsBrokenSingleRecord() throws Exception {
    writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 1, 2, 3 }, 10, 20, "test"));

    writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointEnd();

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

    OLogSequenceNumber checkPointLSN = writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointEnd();

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

    writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointEnd();

    OLogSequenceNumber checkPointLSN = writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointEnd();

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

  public void testLogTruncation() throws Exception {
    writeAheadLog.close();

    OLocalPaginatedStorage paginatedStorage = mock(OLocalPaginatedStorage.class);
    when(paginatedStorage.getName()).thenReturn("WriteAheadLogTest");
    when(paginatedStorage.getStoragePath()).thenReturn(testDir.getAbsolutePath());

    writeAheadLog = new OWriteAheadLog(1024, -1, 2048, 10105, paginatedStorage);

    List<OSetPageDataRecord> writtenRecords = new ArrayList<OSetPageDataRecord>();
    Random rnd = new Random();

    final int recordsToWrite = 220;
    for (int i = 0; i < recordsToWrite; i++) {
      byte[] data = new byte[10];
      rnd.nextBytes(data);

      int pageOffset = rnd.nextInt(65536);
      long pageIndex = rnd.nextLong();
      OSetPageDataRecord setPageDataRecord = new OSetPageDataRecord(data, pageOffset, pageIndex, "test");
      writtenRecords.add(setPageDataRecord);

      writeAheadLog.logRecord(setPageDataRecord);
    }

    assertLogContent(writeAheadLog, writtenRecords.subList(43, writtenRecords.size()));
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(42).getLsn()));

    verify(paginatedStorage).scheduleCheckpoint();
  }

  public void testLogOneCheckPointTruncation() throws Exception {
    writeAheadLog.close();

    OLocalPaginatedStorage paginatedStorage = mock(OLocalPaginatedStorage.class);
    when(paginatedStorage.getName()).thenReturn("WriteAheadLogTest");
    when(paginatedStorage.getStoragePath()).thenReturn(testDir.getAbsolutePath());

    writeAheadLog = new OWriteAheadLog(1024, -1, 2048, 10105, paginatedStorage);

    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();
    Random rnd = new Random();

    final int recordsToWrite = 219;
    OWALRecord walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.logRecord(walRecord);

    writtenRecords.add(walRecord);
    for (int i = 0; i < recordsToWrite; i++) {
      byte[] data = new byte[10];
      rnd.nextBytes(data);

      int pageOffset = rnd.nextInt(65536);
      long pageIndex = rnd.nextLong();
      OSetPageDataRecord setPageDataRecord = new OSetPageDataRecord(data, pageOffset, pageIndex, "test");
      writtenRecords.add(setPageDataRecord);

      writeAheadLog.logRecord(setPageDataRecord);
    }

    assertLogContent(writeAheadLog, writtenRecords.subList(44, writtenRecords.size()));
    Assert.assertNull(writeAheadLog.getLastCheckpoint());
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(43).getLsn()));
    verify(paginatedStorage).scheduleCheckpoint();

    Assert.assertEquals(writeAheadLog.begin(), new OLogSequenceNumber(1, 0));
  }

  public void testLogTwoCheckPointTruncationAllDropped() throws Exception {
    writeAheadLog.close();

    OLocalPaginatedStorage paginatedStorage = mock(OLocalPaginatedStorage.class);
    when(paginatedStorage.getName()).thenReturn("WriteAheadLogTest");
    when(paginatedStorage.getStoragePath()).thenReturn(testDir.getAbsolutePath());

    writeAheadLog = new OWriteAheadLog(1024, -1, 2048, 10105, paginatedStorage);

    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();
    Random rnd = new Random();

    final int recordsToWrite = 218;
    OWALRecord walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    for (int i = 0; i < recordsToWrite; i++) {
      byte[] data = new byte[10];
      rnd.nextBytes(data);

      int pageOffset = rnd.nextInt(65536);
      long pageIndex = rnd.nextLong();
      OSetPageDataRecord setPageDataRecord = new OSetPageDataRecord(data, pageOffset, pageIndex, "test");
      writtenRecords.add(setPageDataRecord);

      writeAheadLog.logRecord(setPageDataRecord);
    }

    assertLogContent(writeAheadLog, writtenRecords.subList(45, writtenRecords.size()));
    Assert.assertNull(writeAheadLog.getLastCheckpoint());
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(44).getLsn()));

    verify(paginatedStorage).scheduleCheckpoint();
    Assert.assertEquals(writeAheadLog.begin(), new OLogSequenceNumber(1, 0));
  }

  public void testLogTwoCheckPointTruncationOneLeft() throws Exception {
    writeAheadLog.close();

    OLocalPaginatedStorage paginatedStorage = mock(OLocalPaginatedStorage.class);
    when(paginatedStorage.getName()).thenReturn("WriteAheadLogTest");
    when(paginatedStorage.getStoragePath()).thenReturn(testDir.getAbsolutePath());

    writeAheadLog = new OWriteAheadLog(1024, -1, 2048, 10105, paginatedStorage);

    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();
    Random rnd = new Random();

    final int recordsToWrite = 219;
    OWALRecord walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    for (int i = 0; i < recordsToWrite; i++) {
      if (i == 50) {
        walRecord = new OFuzzyCheckpointStartRecord();
        writeAheadLog.logRecord(walRecord);
        writtenRecords.add(walRecord);
      } else {
        byte[] data = new byte[10];
        rnd.nextBytes(data);

        int pageOffset = rnd.nextInt(65536);
        long pageIndex = rnd.nextLong();
        OSetPageDataRecord setPageDataRecord = new OSetPageDataRecord(data, pageOffset, pageIndex, "test");
        writtenRecords.add(setPageDataRecord);

        writeAheadLog.logRecord(setPageDataRecord);
      }
    }

    assertLogContent(writeAheadLog, writtenRecords.subList(44, writtenRecords.size()));
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(43).getLsn()));

    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), walRecord.getLsn());
    verify(paginatedStorage).scheduleCheckpoint();
    Assert.assertEquals(writeAheadLog.begin(), new OLogSequenceNumber(1, 0));
  }

  public void testLogThreeCheckPointTruncationAllDropped() throws Exception {
    writeAheadLog.close();

    OLocalPaginatedStorage paginatedStorage = mock(OLocalPaginatedStorage.class);
    when(paginatedStorage.getName()).thenReturn("WriteAheadLogTest");
    when(paginatedStorage.getStoragePath()).thenReturn(testDir.getAbsolutePath());
    writeAheadLog = new OWriteAheadLog(1024, -1, 2048, 10105, paginatedStorage);

    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();
    Random rnd = new Random();

    final int recordsToWrite = 217;
    OWALRecord walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    for (int i = 0; i < recordsToWrite; i++) {
      byte[] data = new byte[10];
      rnd.nextBytes(data);

      int pageOffset = rnd.nextInt(65536);
      long pageIndex = rnd.nextLong();
      OSetPageDataRecord setPageDataRecord = new OSetPageDataRecord(data, pageOffset, pageIndex, "test");
      writtenRecords.add(setPageDataRecord);

      writeAheadLog.logRecord(setPageDataRecord);
    }

    assertLogContent(writeAheadLog, writtenRecords.subList(46, writtenRecords.size()));
    Assert.assertNull(writeAheadLog.getLastCheckpoint());
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(45).getLsn()));

    verify(paginatedStorage).scheduleCheckpoint();
    Assert.assertEquals(writeAheadLog.begin(), new OLogSequenceNumber(1, 0));
  }

  public void testLogThreeCheckPointTruncationOneLeft() throws Exception {
    writeAheadLog.close();

    OLocalPaginatedStorage paginatedStorage = mock(OLocalPaginatedStorage.class);
    when(paginatedStorage.getName()).thenReturn("WriteAheadLogTest");
    when(paginatedStorage.getStoragePath()).thenReturn(testDir.getAbsolutePath());

    writeAheadLog = new OWriteAheadLog(1024, -1, 2048, 10105, paginatedStorage);

    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();
    Random rnd = new Random();

    final int recordsToWrite = 217;
    OWALRecord walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    for (int i = 0; i < recordsToWrite; i++) {
      if (i == 50) {
        walRecord = new OFuzzyCheckpointStartRecord();
        writeAheadLog.logRecord(walRecord);
        writtenRecords.add(walRecord);
      } else {
        byte[] data = new byte[10];
        rnd.nextBytes(data);

        int pageOffset = rnd.nextInt(65536);
        long pageIndex = rnd.nextLong();
        OSetPageDataRecord setPageDataRecord = new OSetPageDataRecord(data, pageOffset, pageIndex, "test");
        writtenRecords.add(setPageDataRecord);

        writeAheadLog.logRecord(setPageDataRecord);
      }
    }
    assertLogContent(writeAheadLog, writtenRecords.subList(45, writtenRecords.size()));
    Assert.assertEquals(walRecord.getLsn(), writeAheadLog.getLastCheckpoint());

    Assert.assertNull(writeAheadLog.read(writtenRecords.get(44).getLsn()));
    verify(paginatedStorage).scheduleCheckpoint();
    Assert.assertEquals(writeAheadLog.begin(), new OLogSequenceNumber(1, 0));
  }

  private void assertLogContent(OWriteAheadLog writeAheadLog, List<? extends OWALRecord> writtenRecords) throws Exception {
    Iterator<? extends OWALRecord> iterator = writtenRecords.iterator();

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
