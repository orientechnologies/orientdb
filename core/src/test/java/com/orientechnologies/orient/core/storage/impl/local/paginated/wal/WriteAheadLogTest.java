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
    if (testDir.exists())
      testDir.delete();
  }

  public void testPageIsBroken() throws Exception {
    writeAheadLog.logRecord(new OSetPageDataRecord(20, "test"));
    writeAheadLog.logRecord(new OSetPageDataRecord(20, "test"));

    OLogSequenceNumber numberToDelete = writeAheadLog.logRecord(new OSetPageDataRecord(20, "test"));

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

    OLogSequenceNumber lsn = writeAheadLog.begin();

    lsn = writeAheadLog.next(lsn);
    lsn = writeAheadLog.next(lsn);
    try {
      writeAheadLog.read(lsn);
      Assert.fail();
    } catch (OWriteAheadLogRecordIsBrokenException e) {
    }

    writeAheadLog.restore();

    lsn = writeAheadLog.begin();
    lsn = writeAheadLog.next(lsn);
    lsn = writeAheadLog.next(lsn);

    Assert.assertNull(lsn);

    Assert.assertNull(writeAheadLog.getLastCheckpoint());
    Assert.assertEquals(writeAheadLog.size(), new File(writeAheadLog.getWalLocation(), "WriteAheadLogTest.0.wal").length());

    writeAheadLog.close();
    writeAheadLog = createWAL();
    Assert.assertNull(writeAheadLog.getLastCheckpoint());
  }

  public void testPageIsBrokenWithSecondMasterRecord() throws Exception {
    writeAheadLog.logRecord(new OSetPageDataRecord(20, "test"));

    OLogSequenceNumber firstCheckPoint = writeAheadLog.logFuzzyCheckPointStart();

    OLogSequenceNumber numberToDelete = writeAheadLog.logRecord(new OSetPageDataRecord(20, "test"));

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

    OLogSequenceNumber lsn = writeAheadLog.begin();

    lsn = writeAheadLog.next(lsn);
    lsn = writeAheadLog.next(lsn);

    try {
      writeAheadLog.read(lsn);
      Assert.fail();
    } catch (OWriteAheadLogRecordIsBrokenException e) {
    }

    writeAheadLog.restore();

    lsn = writeAheadLog.begin();
    lsn = writeAheadLog.next(lsn);
    Assert.assertNull(writeAheadLog.next(lsn));

    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), firstCheckPoint);
    Assert.assertEquals(writeAheadLog.size(), new File(writeAheadLog.getWalLocation(), "WriteAheadLogTest.0.wal").length());
    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), firstCheckPoint);
  }

  public void testPageIsBrokenWithFirstMasterRecord() throws Exception {
    writeAheadLog.logFuzzyCheckPointStart();

    OLogSequenceNumber firstCheckPoint = writeAheadLog.logFuzzyCheckPointStart();

    OLogSequenceNumber numberToDelete = writeAheadLog.logRecord(new OSetPageDataRecord(20, "test"));

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

    OLogSequenceNumber lsn = writeAheadLog.begin();
    lsn = writeAheadLog.next(lsn);
    lsn = writeAheadLog.next(lsn);

    try {
      writeAheadLog.read(lsn);
      Assert.fail();
    } catch (OWriteAheadLogRecordIsBrokenException e) {
    }

    writeAheadLog.restore();

    lsn = writeAheadLog.begin();
    lsn = writeAheadLog.next(lsn);
    lsn = writeAheadLog.next(lsn);
    Assert.assertNull(lsn);

    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), firstCheckPoint);
    Assert.assertEquals(writeAheadLog.size(), new File(writeAheadLog.getWalLocation(), "WriteAheadLogTest.0.wal").length());

    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), firstCheckPoint);
  }

  public void testPageIsBrokenWithBothMasterRecords() throws Exception {
    writeAheadLog.logRecord(new OSetPageDataRecord(20, "test"));
    writeAheadLog.logRecord(new OSetPageDataRecord(20, "test"));

    OLogSequenceNumber numberToDelete = writeAheadLog.logRecord(new OSetPageDataRecord(20, "test"));

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

    OLogSequenceNumber lsn = writeAheadLog.begin();
    lsn = writeAheadLog.next(lsn);
    lsn = writeAheadLog.next(lsn);

    try {
      writeAheadLog.read(lsn);
      Assert.fail();
    } catch (OWriteAheadLogRecordIsBrokenException e) {
    }

    writeAheadLog.restore();

    lsn = writeAheadLog.begin();
    lsn = writeAheadLog.next(lsn);
    lsn = writeAheadLog.next(lsn);

    Assert.assertNull(lsn);

    Assert.assertNull(writeAheadLog.getLastCheckpoint());
    Assert.assertEquals(writeAheadLog.size(), new File(writeAheadLog.getWalLocation(), "WriteAheadLogTest.0.wal").length());

    writeAheadLog.close();
    writeAheadLog = createWAL();

    Assert.assertNull(writeAheadLog.getLastCheckpoint());
  }

  public void testPageIsNotWrittenFully() throws Exception {
    writeAheadLog.logRecord(new OSetPageDataRecord(20, "test"));
    writeAheadLog.logRecord(new OSetPageDataRecord(20, "test"));
    writeAheadLog.logRecord(new OSetPageDataRecord(20, "test"));
    writeAheadLog.logRecord(new OSetPageDataRecord(20, "test"));

    long logSize = writeAheadLog.size();
    writeAheadLog.close();

    RandomAccessFile walFile = new RandomAccessFile(new File(writeAheadLog.getWalLocation(), "WriteAheadLogTest.0.wal"), "rw");
    Assert.assertEquals(logSize, walFile.length());

    walFile.setLength(walFile.length() - 2);
    walFile.close();

    writeAheadLog = createWAL();

    OLogSequenceNumber lsn = writeAheadLog.begin();
    lsn = writeAheadLog.next(lsn);
    lsn = writeAheadLog.next(lsn);
    lsn = writeAheadLog.next(lsn);

    try {
      writeAheadLog.read(lsn);
      Assert.fail();
    } catch (OWriteAheadLogRecordIsBrokenException e) {
    }

    writeAheadLog.restore();

    lsn = writeAheadLog.begin();
    lsn = writeAheadLog.next(lsn);
    lsn = writeAheadLog.next(lsn);
    lsn = writeAheadLog.next(lsn);

    Assert.assertNull(lsn);
    Assert.assertEquals(writeAheadLog.size(), new File(writeAheadLog.getWalLocation(), "WriteAheadLogTest.0.wal").length());
  }

  public void testWriteSingleRecord() throws Exception {

    writeAheadLog.logRecord(new OSetPageDataRecord(20, "test"));

    OWALRecord walRecord = writeAheadLog.read(writeAheadLog.begin());
    Assert.assertTrue(walRecord instanceof OSetPageDataRecord);

    OSetPageDataRecord setPageDataRecord = (OSetPageDataRecord) walRecord;
    Assert.assertEquals(setPageDataRecord.getPageIndex(), 20);
    Assert.assertEquals(setPageDataRecord.getFileName(), "test");

    Assert.assertNull(writeAheadLog.next(walRecord.getLsn()));

    writeAheadLog.close();
    writeAheadLog = createWAL();

    walRecord = writeAheadLog.read(writeAheadLog.begin());
    Assert.assertEquals(walRecord.getLsn(), writeAheadLog.begin());
    Assert.assertTrue(walRecord instanceof OSetPageDataRecord);

    setPageDataRecord = (OSetPageDataRecord) walRecord;
    Assert.assertEquals(setPageDataRecord.getPageIndex(), 20);
    Assert.assertEquals(setPageDataRecord.getFileName(), "test");

    Assert.assertNull(writeAheadLog.next(writeAheadLog.begin()));
  }

  public void testFirstMasterRecordUpdate() throws Exception {
    writeAheadLog.logRecord(new OSetPageDataRecord(20, "test"));
    OLogSequenceNumber masterLSN = writeAheadLog.logFuzzyCheckPointStart();

    writeAheadLog.logFuzzyCheckPointEnd();

    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), masterLSN);
    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), masterLSN);
  }

  public void testSecondMasterRecordUpdate() throws Exception {
    writeAheadLog.logRecord(new OSetPageDataRecord(20, "test"));

    writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.logRecord(new OSetPageDataRecord(20, "test"));

    OLogSequenceNumber checkpointLSN = writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointEnd();

    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), checkpointLSN);
    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), checkpointLSN);
  }

  public void testThirdMasterRecordUpdate() throws Exception {
    writeAheadLog.logRecord(new OSetPageDataRecord(20, "test"));

    writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.logRecord(new OSetPageDataRecord(20, "test"));

    writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.logRecord(new OSetPageDataRecord(20, "test"));

    OLogSequenceNumber checkpointLSN = writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointEnd();

    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), checkpointLSN);
    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), checkpointLSN);
  }

  public void testFirstMasterRecordIsBrokenSingleRecord() throws Exception {
    writeAheadLog.logRecord(new OSetPageDataRecord(20, "test"));

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
    writeAheadLog.logRecord(new OSetPageDataRecord(20, "test"));

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
    writeAheadLog.logRecord(new OSetPageDataRecord(20, "test"));

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
      long pageIndex = rnd.nextLong();
      OSetPageDataRecord setPageDataRecord = new OSetPageDataRecord(pageIndex, "test");
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
      long pageIndex = rnd.nextLong();
      OSetPageDataRecord setPageDataRecord = new OSetPageDataRecord(pageIndex, "test");
      writtenRecords.add(setPageDataRecord);

      writeAheadLog.logRecord(setPageDataRecord);
    }

    writeAheadLog.close();
    writeAheadLog = createWAL();

    for (int i = 0; i < recordsToWrite; i++) {
      long pageIndex = rnd.nextLong();
      OSetPageDataRecord setPageDataRecord = new OSetPageDataRecord(pageIndex, "test");
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

    writeAheadLog = new OWriteAheadLog(1024, -1, 2048, 2 * 2048, paginatedStorage);

    List<OSetPageDataRecord> writtenRecords = new ArrayList<OSetPageDataRecord>();
    Random rnd = new Random();

    OSetPageDataRecord setPageDataRecord = new OSetPageDataRecord(1, "test");

    int oneSegment = 2048 / serializeSize(setPageDataRecord);
    int recordsToWrite = 3 * oneSegment;

    for (int i = 0; i < recordsToWrite; i++) {
      long pageIndex = rnd.nextLong();
      setPageDataRecord = new OSetPageDataRecord(pageIndex, "test");

      writtenRecords.add(setPageDataRecord);
      writeAheadLog.logRecord(setPageDataRecord);
    }

    assertLogContent(writeAheadLog, writtenRecords.subList(oneSegment, writtenRecords.size()));
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(oneSegment - 1).getLsn()));

    verify(paginatedStorage).scheduleCheckpoint();
  }

  public void testLogOneCheckPointTruncation() throws Exception {
    writeAheadLog.close();

    OLocalPaginatedStorage paginatedStorage = mock(OLocalPaginatedStorage.class);
    when(paginatedStorage.getName()).thenReturn("WriteAheadLogTest");
    when(paginatedStorage.getStoragePath()).thenReturn(testDir.getAbsolutePath());

    writeAheadLog = new OWriteAheadLog(1024, -1, 2048, 2 * 2048, paginatedStorage);

    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    OSetPageDataRecord setPageDataRecord = new OSetPageDataRecord(256, "test");

    final int recordsToWriteOneSegment = 2048 / serializeSize(setPageDataRecord);
    final int recordsToWrite = 2 * recordsToWriteOneSegment;

    OWALRecord walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.logRecord(walRecord);

    writtenRecords.add(walRecord);

    final int firstRecordsToWrite = (2048 - serializeSize(walRecord)) / serializeSize(setPageDataRecord);

    for (int i = 0; i < firstRecordsToWrite; i++) {
      setPageDataRecord = new OSetPageDataRecord(256, "test");
      writtenRecords.add(setPageDataRecord);

      writeAheadLog.logRecord(setPageDataRecord);
    }

    for (int i = 0; i < recordsToWrite; i++) {
      setPageDataRecord = new OSetPageDataRecord(256, "test");
      writtenRecords.add(setPageDataRecord);

      writeAheadLog.logRecord(setPageDataRecord);
    }

    assertLogContent(writeAheadLog, writtenRecords.subList(firstRecordsToWrite + 1, writtenRecords.size()));
    Assert.assertNull(writeAheadLog.getLastCheckpoint());
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(firstRecordsToWrite).getLsn()));
    verify(paginatedStorage).scheduleCheckpoint();

    Assert.assertEquals(writeAheadLog.begin(), new OLogSequenceNumber(1, 0));
  }

  public void testLogTwoCheckPointTruncationAllDropped() throws Exception {
    writeAheadLog.close();

    OLocalPaginatedStorage paginatedStorage = mock(OLocalPaginatedStorage.class);
    when(paginatedStorage.getName()).thenReturn("WriteAheadLogTest");
    when(paginatedStorage.getStoragePath()).thenReturn(testDir.getAbsolutePath());

    writeAheadLog = new OWriteAheadLog(1024, -1, 2048, 2 * 2048, paginatedStorage);

    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();
    Random rnd = new Random();

    OWALRecord walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    OSetPageDataRecord setPageDataRecord = new OSetPageDataRecord(1, "test");
    final int firstRecordsToWrite = (2048 - 2 * serializeSize(walRecord)) / serializeSize(setPageDataRecord);

    for (int i = 0; i < firstRecordsToWrite; i++) {
      long pageIndex = rnd.nextLong();
      setPageDataRecord = new OSetPageDataRecord(pageIndex, "test");

      writtenRecords.add(setPageDataRecord);
      writeAheadLog.logRecord(setPageDataRecord);
    }

    int recordsToWrite = 2 * (2048 / serializeSize(setPageDataRecord));

    for (int i = 0; i < recordsToWrite; i++) {
      long pageIndex = rnd.nextLong();
      setPageDataRecord = new OSetPageDataRecord(pageIndex, "test");

      writtenRecords.add(setPageDataRecord);
      writeAheadLog.logRecord(setPageDataRecord);
    }

    assertLogContent(writeAheadLog, writtenRecords.subList(firstRecordsToWrite + 2, writtenRecords.size()));
    Assert.assertNull(writeAheadLog.getLastCheckpoint());
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(firstRecordsToWrite + 1).getLsn()));

    verify(paginatedStorage).scheduleCheckpoint();
    Assert.assertEquals(writeAheadLog.begin(), new OLogSequenceNumber(1, 0));
  }

  public void testLogTwoCheckPointTruncationOneLeft() throws Exception {
    writeAheadLog.close();

    OLocalPaginatedStorage paginatedStorage = mock(OLocalPaginatedStorage.class);
    when(paginatedStorage.getName()).thenReturn("WriteAheadLogTest");
    when(paginatedStorage.getStoragePath()).thenReturn(testDir.getAbsolutePath());

    writeAheadLog = new OWriteAheadLog(1024, -1, 2048, 2 * 2048, paginatedStorage);

    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();
    Random rnd = new Random();

    OWALRecord walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    OSetPageDataRecord setPageDataRecord = new OSetPageDataRecord(1, "test");
    final int firstRecordsToWrite = (2048 - serializeSize(walRecord)) / serializeSize(setPageDataRecord);

    for (int i = 0; i < firstRecordsToWrite; i++) {
      long pageIndex = rnd.nextLong();
      setPageDataRecord = new OSetPageDataRecord(pageIndex, "test");

      writtenRecords.add(setPageDataRecord);
      writeAheadLog.logRecord(setPageDataRecord);
    }

    int recordsToWrite = 2 * (2048 / serializeSize(setPageDataRecord));

    for (int i = 0; i < recordsToWrite - 1; i++) {
      long pageIndex = rnd.nextLong();
      setPageDataRecord = new OSetPageDataRecord(pageIndex, "test");

      writtenRecords.add(setPageDataRecord);
      writeAheadLog.logRecord(setPageDataRecord);
    }

    walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    assertLogContent(writeAheadLog, writtenRecords.subList(firstRecordsToWrite + 1, writtenRecords.size()));
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(firstRecordsToWrite).getLsn()));

    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), walRecord.getLsn());
    verify(paginatedStorage).scheduleCheckpoint();
    Assert.assertEquals(writeAheadLog.begin(), new OLogSequenceNumber(1, 0));
  }

  public void testLogThreeCheckPointTruncationAllDropped() throws Exception {
    writeAheadLog.close();

    OLocalPaginatedStorage paginatedStorage = mock(OLocalPaginatedStorage.class);
    when(paginatedStorage.getName()).thenReturn("WriteAheadLogTest");
    when(paginatedStorage.getStoragePath()).thenReturn(testDir.getAbsolutePath());
    writeAheadLog = new OWriteAheadLog(1024, -1, 2048, 2 * 2048, paginatedStorage);

    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();
    Random rnd = new Random();

    OWALRecord walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    OSetPageDataRecord setPageDataRecord = new OSetPageDataRecord(1, "test");
    final int firstRecordsToWrite = (2048 - 3 * serializeSize(walRecord)) / (serializeSize(setPageDataRecord));

    for (int i = 0; i < firstRecordsToWrite; i++) {
      long pageIndex = rnd.nextLong();
      setPageDataRecord = new OSetPageDataRecord(pageIndex, "test");
      writtenRecords.add(setPageDataRecord);

      writeAheadLog.logRecord(setPageDataRecord);
    }

    final int recordsToWriteOneSegment = 2048 / serializeSize(setPageDataRecord);
    final int recordsToWrite = 2 * recordsToWriteOneSegment;

    for (int i = 0; i < recordsToWrite; i++) {
      long pageIndex = rnd.nextLong();
      setPageDataRecord = new OSetPageDataRecord(pageIndex, "test");
      writtenRecords.add(setPageDataRecord);

      writeAheadLog.logRecord(setPageDataRecord);
    }

    assertLogContent(writeAheadLog, writtenRecords.subList(firstRecordsToWrite + 3, writtenRecords.size()));
    Assert.assertNull(writeAheadLog.getLastCheckpoint());
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(firstRecordsToWrite + 2).getLsn()));

    verify(paginatedStorage).scheduleCheckpoint();
    Assert.assertEquals(writeAheadLog.begin(), new OLogSequenceNumber(1, 0));
  }

  public void testLogThreeCheckPointTruncationOneLeft() throws Exception {
    writeAheadLog.close();

    OLocalPaginatedStorage paginatedStorage = mock(OLocalPaginatedStorage.class);
    when(paginatedStorage.getName()).thenReturn("WriteAheadLogTest");
    when(paginatedStorage.getStoragePath()).thenReturn(testDir.getAbsolutePath());

    writeAheadLog = new OWriteAheadLog(1024, -1, 2048, 2 * 2048, paginatedStorage);

    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();
    Random rnd = new Random();

    OWALRecord walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    OSetPageDataRecord setPageDataRecord = new OSetPageDataRecord(1, "test");
    final int firstRecordsToWrite = (2048 - 2 * serializeSize(walRecord)) / serializeSize(setPageDataRecord);

    for (int i = 0; i < firstRecordsToWrite; i++) {
      long pageIndex = rnd.nextLong();
      setPageDataRecord = new OSetPageDataRecord(pageIndex, "test");

      writtenRecords.add(setPageDataRecord);
      writeAheadLog.logRecord(setPageDataRecord);
    }

    int recordsToWrite = 2 * (2048 / serializeSize(setPageDataRecord));

    for (int i = 0; i < recordsToWrite - 1; i++) {
      long pageIndex = rnd.nextLong();
      setPageDataRecord = new OSetPageDataRecord(pageIndex, "test");

      writtenRecords.add(setPageDataRecord);
      writeAheadLog.logRecord(setPageDataRecord);
    }

    walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    assertLogContent(writeAheadLog, writtenRecords.subList(firstRecordsToWrite + 2, writtenRecords.size()));
    Assert.assertEquals(walRecord.getLsn(), writeAheadLog.getLastCheckpoint());

    Assert.assertNull(writeAheadLog.read(writtenRecords.get(firstRecordsToWrite + 1).getLsn()));
    verify(paginatedStorage).scheduleCheckpoint();
    Assert.assertEquals(writeAheadLog.begin(), new OLogSequenceNumber(1, 0));
  }

  public void flushTillLSN() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();
    Random rnd = new Random();

    final int recordsToWrite = 80;
    for (int i = 0; i < recordsToWrite; i++) {
      long pageIndex = rnd.nextLong();
      OSetPageDataRecord setPageDataRecord = new OSetPageDataRecord(pageIndex, "test");
      writtenRecords.add(setPageDataRecord);

      writeAheadLog.logRecord(setPageDataRecord);
    }

    writeAheadLog.flushTillLSN(writtenRecords.get(70).getLsn());
    writeAheadLog.close(false);

    writeAheadLog = createWAL();
    assertLogContent(writeAheadLog, writtenRecords.subList(0, 71));
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(71).getLsn()));
  }

  public void flushTillLSNFullBufferFlush() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();
    Random rnd = new Random();

    final int recordsToWrite = 80;
    for (int i = 0; i < recordsToWrite; i++) {
      long pageIndex = rnd.nextLong();
      OSetPageDataRecord setPageDataRecord = new OSetPageDataRecord(pageIndex, "test");
      writtenRecords.add(setPageDataRecord);

      writeAheadLog.logRecord(setPageDataRecord);
    }

    writeAheadLog.flushTillLSN(writtenRecords.get(70).getLsn());
    writeAheadLog.close();

    writeAheadLog = createWAL();
    assertLogContent(writeAheadLog, writtenRecords);
  }

  private void assertLogContent(OWriteAheadLog writeAheadLog, List<? extends OWALRecord> writtenRecords) throws Exception {
    Iterator<? extends OWALRecord> iterator = writtenRecords.iterator();

    OWALRecord writtenRecord = iterator.next();
    OWALRecord readRecord = writeAheadLog.read(writtenRecord.getLsn());

    Assert.assertEquals(writtenRecord, readRecord);
    while (iterator.hasNext()) {
      OLogSequenceNumber lsn = writeAheadLog.next(readRecord.getLsn());
      readRecord = writeAheadLog.read(lsn);
      writtenRecord = iterator.next();

      Assert.assertEquals(writtenRecord, readRecord);
    }

    Assert.assertNull(writeAheadLog.next(readRecord.getLsn()));
  }

  private int serializeSize(OWALRecord walRecord) {
    return walRecord.serializedSize() + 1 + 2 * OIntegerSerializer.INT_SIZE;
  }
}
