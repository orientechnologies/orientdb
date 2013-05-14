package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
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
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;

/**
 * @author Andrey Lomakin
 * @since 29.04.13
 */
@Test
public class WriteAheadLogTest {
  private static final int ONE_KB = 1024;
  private OWriteAheadLog   writeAheadLog;
  private File             testDir;

  @BeforeClass
  public void beforeClass() {
    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null || buildDirectory.isEmpty())
      buildDirectory = ".";

    testDir = new File(buildDirectory, "writeAheadLogTest");
    if (!testDir.exists())
      testDir.mkdir();

    OWALRecordsFactory.INSTANCE.registerNewRecord((byte) 128, TestRecord.class);
  }

  @BeforeMethod
  public void beforeMethod() throws Exception {
    writeAheadLog = createWAL();
  }

  private OWriteAheadLog createWAL() throws IOException {
    OLocalPaginatedStorage paginatedStorage = mock(OLocalPaginatedStorage.class);
    when(paginatedStorage.getName()).thenReturn("WriteAheadLogTest");
    when(paginatedStorage.getStoragePath()).thenReturn(testDir.getAbsolutePath());

    return new OWriteAheadLog(2, -1, OWALPage.PAGE_SIZE * 4, 100L * 1024L * 1024L * 1024L, paginatedStorage);
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

  public void testWriteSingleRecord() throws Exception {

    writeAheadLog.logRecord(new OUpdatePageRecord(20, "test"));

    OWALRecord walRecord = writeAheadLog.read(writeAheadLog.begin());
    Assert.assertTrue(walRecord instanceof OUpdatePageRecord);

    OUpdatePageRecord setPageDataRecord = (OUpdatePageRecord) walRecord;
    Assert.assertEquals(setPageDataRecord.getPageIndex(), 20);
    Assert.assertEquals(setPageDataRecord.getFileName(), "test");

    Assert.assertNull(writeAheadLog.next(walRecord.getLsn()));

    writeAheadLog.close();
    writeAheadLog = createWAL();

    walRecord = writeAheadLog.read(writeAheadLog.begin());
    Assert.assertEquals(walRecord.getLsn(), writeAheadLog.begin());
    Assert.assertTrue(walRecord instanceof OUpdatePageRecord);

    setPageDataRecord = (OUpdatePageRecord) walRecord;
    Assert.assertEquals(setPageDataRecord.getPageIndex(), 20);
    Assert.assertEquals(setPageDataRecord.getFileName(), "test");

    Assert.assertNull(writeAheadLog.next(writeAheadLog.begin()));
  }

  public void testFirstMasterRecordUpdate() throws Exception {
    writeAheadLog.logRecord(new OUpdatePageRecord(20, "test"));
    OLogSequenceNumber masterLSN = writeAheadLog.logFuzzyCheckPointStart();

    writeAheadLog.logFuzzyCheckPointEnd();

    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), masterLSN);
    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), masterLSN);
  }

  public void testSecondMasterRecordUpdate() throws Exception {
    writeAheadLog.logRecord(new OUpdatePageRecord(20, "test"));

    writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.logRecord(new OUpdatePageRecord(20, "test"));

    OLogSequenceNumber checkpointLSN = writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointEnd();

    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), checkpointLSN);
    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), checkpointLSN);
  }

  public void testThirdMasterRecordUpdate() throws Exception {
    writeAheadLog.logRecord(new OUpdatePageRecord(20, "test"));

    writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.logRecord(new OUpdatePageRecord(20, "test"));

    writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.logRecord(new OUpdatePageRecord(20, "test"));

    OLogSequenceNumber checkpointLSN = writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointEnd();

    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), checkpointLSN);
    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), checkpointLSN);
  }

  public void testWriteMultipleRecordsWithDifferentSizes() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    Assert.assertEquals(writeAheadLog.size(), 0);

    long logSize = 0;
    int contentSize;
    // first page
    contentSize = ONE_KB;
    OWALRecord walRecord = new TestRecord(contentSize);
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);
    logSize += OWALPage.RECORDS_OFFSET + contentSize;
    Assert.assertEquals(writeAheadLog.size(), logSize);

    contentSize = ONE_KB;
    walRecord = new TestRecord(contentSize);
    writeAheadLog.logRecord(walRecord);
    logSize += contentSize;
    Assert.assertEquals(writeAheadLog.size(), logSize);
    writtenRecords.add(walRecord);

    contentSize = OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET - 2048 - OWALPage.MIN_RECORD_SIZE + 1;
    walRecord = new TestRecord(contentSize);
    writeAheadLog.logRecord(walRecord);

    logSize += contentSize;
    Assert.assertEquals(writeAheadLog.size(), logSize);
    writtenRecords.add(walRecord);

    // second page
    contentSize = ONE_KB;
    walRecord = new TestRecord(contentSize);
    writeAheadLog.logRecord(walRecord);
    logSize += OWALPage.MIN_RECORD_SIZE - 1 + OWALPage.RECORDS_OFFSET + contentSize;
    Assert.assertEquals(writeAheadLog.size(), logSize);
    writtenRecords.add(walRecord);

    contentSize = ONE_KB;
    walRecord = new TestRecord(contentSize);
    writeAheadLog.logRecord(walRecord);
    logSize += contentSize;
    Assert.assertEquals(writeAheadLog.size(), logSize);
    writtenRecords.add(walRecord);

    contentSize = OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET - 2048 - OWALPage.MIN_RECORD_SIZE;
    walRecord = new TestRecord(contentSize);
    writeAheadLog.logRecord(walRecord);
    logSize += contentSize;
    Assert.assertEquals(writeAheadLog.size(), logSize);
    writtenRecords.add(walRecord);

    // third page
    contentSize = ONE_KB;
    walRecord = new TestRecord(contentSize);
    writeAheadLog.logRecord(walRecord);
    logSize += contentSize - 1 + OWALPage.MIN_RECORD_SIZE + OWALPage.RECORDS_OFFSET;
    Assert.assertEquals(writeAheadLog.size(), logSize);
    writtenRecords.add(walRecord);

    contentSize = ONE_KB;
    walRecord = new TestRecord(contentSize);
    writeAheadLog.logRecord(walRecord);
    logSize += contentSize;
    Assert.assertEquals(writeAheadLog.size(), logSize);
    writtenRecords.add(walRecord);

    contentSize = OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET - 2047 - OWALPage.MIN_RECORD_SIZE;
    walRecord = new TestRecord(contentSize);
    writeAheadLog.logRecord(walRecord);
    logSize += contentSize;
    writtenRecords.add(walRecord);

    // fourth page
    contentSize = ONE_KB;
    walRecord = new TestRecord(contentSize);
    writeAheadLog.logRecord(walRecord);
    logSize += contentSize - 1 + OWALPage.MIN_RECORD_SIZE + OWALPage.RECORDS_OFFSET;
    Assert.assertEquals(writeAheadLog.size(), logSize);
    writtenRecords.add(walRecord);

    contentSize = ONE_KB;
    walRecord = new TestRecord(contentSize);
    writeAheadLog.logRecord(walRecord);
    logSize += contentSize;
    Assert.assertEquals(writeAheadLog.size(), logSize);
    writtenRecords.add(walRecord);

    contentSize = OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET - 2047;
    walRecord = new TestRecord(contentSize);
    writeAheadLog.logRecord(walRecord);
    logSize += contentSize;
    Assert.assertEquals(writeAheadLog.size(), logSize);
    writtenRecords.add(walRecord);

    // fifth page
    contentSize = ONE_KB;
    walRecord = new TestRecord(contentSize);
    writeAheadLog.logRecord(walRecord);
    logSize += contentSize + OWALPage.RECORDS_OFFSET;
    Assert.assertEquals(writeAheadLog.size(), logSize);
    writtenRecords.add(walRecord);

    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.size(), logSize);

    assertLogContent(writeAheadLog, writtenRecords);
  }

  public void testWriteMultipleRecordsWithDifferentSizeAfterCloseOne() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    // first page
    OWALRecord walRecord = new TestRecord(ONE_KB);
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(ONE_KB);
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    long seed = System.currentTimeMillis();
    System.out.println("seed of testWriteMultipleRecordsWithDifferentSizeAfterCloseOne " + seed);
    Random random = new Random(seed);
    writeAheadLog = createWAL();

    for (int writtenSize = 0; writtenSize < 4 * OWALPage.PAGE_SIZE;) {
      int contentSize = random.nextInt(2 * OWALPage.PAGE_SIZE - 1) + 1;
      walRecord = new TestRecord(contentSize);

      writeAheadLog.logRecord(walRecord);
      writtenRecords.add(walRecord);

      writtenSize += contentSize;
    }

    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    writeAheadLog = createWAL();

    assertLogContent(writeAheadLog, writtenRecords);
  }

  public void testWriteMultipleRecordsWithDifferentSizeAfterCloseTwo() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    // first page
    OWALRecord walRecord = new TestRecord(ONE_KB);
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(ONE_KB);
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET - 2048 - OWALPage.MIN_RECORD_SIZE + 1);
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    long seed = System.currentTimeMillis();
    System.out.println("seed of testWriteMultipleRecordsWithDifferentSizeAfterCloseTwo " + seed);
    Random random = new Random(seed);
    writeAheadLog = createWAL();

    for (int writtenSize = 0; writtenSize < 4 * OWALPage.PAGE_SIZE;) {
      int contentSize = random.nextInt(2 * OWALPage.PAGE_SIZE - 1) + 1;
      walRecord = new TestRecord(contentSize);

      writeAheadLog.logRecord(walRecord);
      writtenRecords.add(walRecord);

      writtenSize += contentSize;
    }

    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    writeAheadLog = createWAL();

    assertLogContent(writeAheadLog, writtenRecords);
  }

  public void testWriteMultipleRecordsWithDifferentSizeAfterCloseThree() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    // first page
    OWALRecord walRecord = new TestRecord(ONE_KB);
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(ONE_KB);
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET - 2048 - OWALPage.MIN_RECORD_SIZE);
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    long seed = System.currentTimeMillis();
    System.out.println("seed of testWriteMultipleRecordsWithDifferentSizeAfterCloseThree " + seed);
    Random random = new Random(seed);
    writeAheadLog = createWAL();

    for (int writtenSize = 0; writtenSize < 4 * OWALPage.PAGE_SIZE;) {
      int contentSize = random.nextInt(2 * OWALPage.PAGE_SIZE - 1) + 1;
      walRecord = new TestRecord(contentSize);

      writeAheadLog.logRecord(walRecord);
      writtenRecords.add(walRecord);

      writtenSize += contentSize;
    }

    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    writeAheadLog = createWAL();

    assertLogContent(writeAheadLog, writtenRecords);
  }

  public void testWriteMultipleRecordsWithDifferentSizeAfterCloseFour() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    // first page
    OWALRecord walRecord = new TestRecord(ONE_KB);
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(ONE_KB);
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET - 2048);
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    long seed = System.currentTimeMillis();
    System.out.println("seed of testWriteMultipleRecordsWithDifferentSizeAfterCloseFour " + seed);
    Random random = new Random(seed);
    writeAheadLog = createWAL();

    for (int writtenSize = 0; writtenSize < 4 * OWALPage.PAGE_SIZE;) {
      int contentSize = random.nextInt(2 * OWALPage.PAGE_SIZE - 1) + 1;
      walRecord = new TestRecord(contentSize);

      writeAheadLog.logRecord(walRecord);
      writtenRecords.add(walRecord);

      writtenSize += contentSize;
    }

    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    writeAheadLog = createWAL();

    assertLogContent(writeAheadLog, writtenRecords);
  }

  public void testWriteMultipleRandomRecords() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    long seed = System.currentTimeMillis();
    System.out.println("seed of testWriteMultipleRecordsWithDifferentSizeAfterCloseFour " + seed);
    Random random = new Random(seed);

    for (int writtenSize = 0; writtenSize < 16 * OWALPage.PAGE_SIZE;) {
      int contentSize = random.nextInt(2 * OWALPage.PAGE_SIZE - 1) + 1;
      OWALRecord walRecord = new TestRecord(contentSize);

      writeAheadLog.logRecord(walRecord);
      writtenRecords.add(walRecord);

      writtenSize += contentSize;
    }

    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    writeAheadLog = createWAL();

    assertLogContent(writeAheadLog, writtenRecords);

    for (int writtenSize = 0; writtenSize < 16 * OWALPage.PAGE_SIZE;) {
      int contentSize = random.nextInt(2 * OWALPage.PAGE_SIZE - 1) + 1;
      OWALRecord walRecord = new TestRecord(contentSize);

      writeAheadLog.logRecord(walRecord);
      writtenRecords.add(walRecord);

      writtenSize += contentSize;
    }

    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    writeAheadLog = createWAL();

    assertLogContent(writeAheadLog, writtenRecords);

  }

  public void testFlushedLSNOnePage() throws Exception {
    OWALRecord walRecord = new TestRecord(ONE_KB);
    writeAheadLog.logRecord(walRecord);

    walRecord = new TestRecord(ONE_KB);
    writeAheadLog.logRecord(walRecord);

    Assert.assertNull(writeAheadLog.getFlushedLSN());

    writeAheadLog.flush();

    Assert.assertEquals(writeAheadLog.getFlushedLSN(), walRecord.getLsn());

    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.getFlushedLSN(), walRecord.getLsn());
  }

  public void testFlushedLSNOnePageWithLessThanMinRecordSpace() throws Exception {
    OWALRecord walRecord = new TestRecord(ONE_KB);
    writeAheadLog.logRecord(walRecord);

    walRecord = new TestRecord(ONE_KB);
    writeAheadLog.logRecord(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET - 2048 - OWALPage.MIN_RECORD_SIZE + 1);
    writeAheadLog.logRecord(walRecord);

    Assert.assertNull(writeAheadLog.getFlushedLSN());

    writeAheadLog.flush();

    Assert.assertEquals(writeAheadLog.getFlushedLSN(), walRecord.getLsn());

    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.getFlushedLSN(), walRecord.getLsn());
  }

  public void testFlushedLSNOnePageWithMinRecordSpace() throws Exception {
    OWALRecord walRecord = new TestRecord(ONE_KB);
    writeAheadLog.logRecord(walRecord);

    walRecord = new TestRecord(ONE_KB);
    writeAheadLog.logRecord(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET - 2048 - OWALPage.MIN_RECORD_SIZE);
    writeAheadLog.logRecord(walRecord);

    Assert.assertNull(writeAheadLog.getFlushedLSN());

    writeAheadLog.flush();

    Assert.assertEquals(writeAheadLog.getFlushedLSN(), walRecord.getLsn());

    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.getFlushedLSN(), walRecord.getLsn());
  }

  public void testFlushedLSNOnePageWithNoSpace() throws Exception {
    OWALRecord walRecord = new TestRecord(ONE_KB);
    writeAheadLog.logRecord(walRecord);

    walRecord = new TestRecord(ONE_KB);
    writeAheadLog.logRecord(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET - 2048);
    writeAheadLog.logRecord(walRecord);

    Assert.assertNull(writeAheadLog.getFlushedLSN());

    writeAheadLog.flush();

    Assert.assertEquals(writeAheadLog.getFlushedLSN(), walRecord.getLsn());

    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.getFlushedLSN(), walRecord.getLsn());
  }

  public void testFlushedLSNTwoPagesOneWithTrail() throws Exception {
    OWALRecord walRecord = new TestRecord(ONE_KB);
    writeAheadLog.logRecord(walRecord);

    walRecord = new TestRecord(ONE_KB);
    writeAheadLog.logRecord(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET - 2048 - OWALPage.MIN_RECORD_SIZE);
    writeAheadLog.logRecord(walRecord);

    walRecord = new TestRecord(ONE_KB);
    writeAheadLog.logRecord(walRecord);

    Assert.assertNull(writeAheadLog.getFlushedLSN());

    writeAheadLog.flush();

    Assert.assertEquals(writeAheadLog.getFlushedLSN(), walRecord.getLsn());

    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.getFlushedLSN(), walRecord.getLsn());
  }

  public void testFlushedLSNTwoTwoSegments() throws Exception {
    long seek = System.currentTimeMillis();
    System.out.println("testFlushedLSNTwoTwoSegments seek " + seek);
    Random random = new Random(seek);

    int writtenContent = 0;
    OWALRecord walRecord;

    while (writtenContent <= 4 * OWALPage.PAGE_SIZE) {
      int contentSize = random.nextInt(OWALPage.PAGE_SIZE - 1) + 1;
      walRecord = new TestRecord(contentSize);
      writeAheadLog.logRecord(walRecord);

      writtenContent += contentSize;
    }

    int contentSize = random.nextInt(OWALPage.PAGE_SIZE - 1) + 1;
    walRecord = new TestRecord(contentSize);
    writeAheadLog.logRecord(walRecord);

    writeAheadLog.flush();

    Assert.assertEquals(writeAheadLog.getFlushedLSN(), walRecord.getLsn());

    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.getFlushedLSN(), walRecord.getLsn());
  }

  public void testFirstMasterRecordIsBrokenSingleRecord() throws Exception {
    writeAheadLog.logRecord(new OUpdatePageRecord(20, "test"));

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
    writeAheadLog.logRecord(new OUpdatePageRecord(20, "test"));

    OLogSequenceNumber checkPointLSN = writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.logRecord(new TestRecord(OWALPage.PAGE_SIZE));

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
    writeAheadLog.logRecord(new OUpdatePageRecord(20, "test"));

    writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.logRecord(new TestRecord(OWALPage.PAGE_SIZE));

    OLogSequenceNumber checkPointLSN = writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.logRecord(new TestRecord(OWALPage.PAGE_SIZE));

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
    List<OUpdatePageRecord> writtenRecords = new ArrayList<OUpdatePageRecord>();
    Random rnd = new Random();

    final int recordsToWrite = 2048;
    for (int i = 0; i < recordsToWrite; i++) {
      long pageIndex = rnd.nextLong();
      OUpdatePageRecord setPageDataRecord = new OUpdatePageRecord(pageIndex, "test");
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
    List<OUpdatePageRecord> writtenRecords = new ArrayList<OUpdatePageRecord>();
    Random rnd = new Random();

    final int recordsToWrite = 1;
    for (int i = 0; i < recordsToWrite; i++) {
      long pageIndex = rnd.nextLong();
      OUpdatePageRecord setPageDataRecord = new OUpdatePageRecord(pageIndex, "test");
      writtenRecords.add(setPageDataRecord);

      writeAheadLog.logRecord(setPageDataRecord);
    }

    writeAheadLog.close();
    writeAheadLog = createWAL();

    for (int i = 0; i < recordsToWrite; i++) {
      long pageIndex = rnd.nextLong();
      OUpdatePageRecord setPageDataRecord = new OUpdatePageRecord(pageIndex, "test");
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

    writeAheadLog = new OWriteAheadLog(2, -1, 2 * OWALPage.PAGE_SIZE, 4 * OWALPage.PAGE_SIZE, paginatedStorage);

    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();
    long seed = System.currentTimeMillis();
    System.out.println("testLogTruncation seed " + seed);

    Random rnd = new Random(seed);
    long logSize = writeAheadLog.size() + 1;
    long prevLogSize = 0;

    int firstSegmentIndex = -1;
    int counter = 0;
    while (logSize > prevLogSize) {
      int contentSize = rnd.nextInt(OWALPage.PAGE_SIZE - 128) + 128;
      OWALRecord walRecord = new TestRecord(contentSize);

      writeAheadLog.logRecord(walRecord);
      writtenRecords.add(walRecord);

      prevLogSize = logSize;
      logSize = writeAheadLog.size();

      if (firstSegmentIndex < 0 && logSize > 2 * OWALPage.PAGE_SIZE)
        firstSegmentIndex = counter;

      counter++;
    }

    assertLogContent(writeAheadLog, writtenRecords.subList(firstSegmentIndex + 1, writtenRecords.size()));

    Assert.assertNull(writeAheadLog.read(writtenRecords.get(firstSegmentIndex).getLsn()));

    verify(paginatedStorage).scheduleCheckpoint();
  }

  public void testLogOneCheckPointTruncation() throws Exception {
    writeAheadLog.close();

    OLocalPaginatedStorage paginatedStorage = mock(OLocalPaginatedStorage.class);
    when(paginatedStorage.getName()).thenReturn("WriteAheadLogTest");
    when(paginatedStorage.getStoragePath()).thenReturn(testDir.getAbsolutePath());

    writeAheadLog = new OWriteAheadLog(2, -1, 2 * OWALPage.PAGE_SIZE, 4 * OWALPage.PAGE_SIZE, paginatedStorage);

    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    OWALRecord walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.logRecord(walRecord);

    writtenRecords.add(walRecord);

    long seed = System.currentTimeMillis();
    System.out.println("testLogOneCheckPointTruncation seed " + seed);

    Random rnd = new Random(seed);

    int firstSegmentIndex = -1;
    int counter = 1;

    long logSize = writeAheadLog.size() + 1;
    long prevLogSize = 0;

    while (logSize > prevLogSize) {
      int contentSize = rnd.nextInt(OWALPage.PAGE_SIZE - 128) + 128;
      walRecord = new TestRecord(contentSize);

      writeAheadLog.logRecord(walRecord);
      writtenRecords.add(walRecord);

      prevLogSize = logSize;
      logSize = writeAheadLog.size();

      if (firstSegmentIndex < 0 && logSize > 2 * OWALPage.PAGE_SIZE)
        firstSegmentIndex = counter;

      counter++;
    }

    assertLogContent(writeAheadLog, writtenRecords.subList(firstSegmentIndex + 1, writtenRecords.size()));
    Assert.assertNull(writeAheadLog.getLastCheckpoint());
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(firstSegmentIndex).getLsn()));
    verify(paginatedStorage).scheduleCheckpoint();

    Assert.assertEquals(writeAheadLog.begin(), writtenRecords.get(firstSegmentIndex + 1).getLsn());
  }

  public void testLogTwoCheckPointTruncationAllDropped() throws Exception {
    writeAheadLog.close();

    OLocalPaginatedStorage paginatedStorage = mock(OLocalPaginatedStorage.class);
    when(paginatedStorage.getName()).thenReturn("WriteAheadLogTest");
    when(paginatedStorage.getStoragePath()).thenReturn(testDir.getAbsolutePath());

    writeAheadLog = new OWriteAheadLog(2, -1, 2 * OWALPage.PAGE_SIZE, 4 * OWALPage.PAGE_SIZE, paginatedStorage);

    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();
    long seed = System.currentTimeMillis();
    System.out.println("testLogTwoCheckPointTruncationAllDropped seed " + seed);

    Random rnd = new Random(seed);

    OWALRecord walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    int firstSegmentIndex = -1;
    int counter = 2;

    long logSize = writeAheadLog.size() + 1;
    long prevLogSize = 0;

    while (logSize > prevLogSize) {
      int contentSize = rnd.nextInt(OWALPage.PAGE_SIZE - 128) + 128;
      walRecord = new TestRecord(contentSize);

      writeAheadLog.logRecord(walRecord);
      writtenRecords.add(walRecord);

      prevLogSize = logSize;
      logSize = writeAheadLog.size();

      if (firstSegmentIndex < 0 && logSize > 2 * OWALPage.PAGE_SIZE)
        firstSegmentIndex = counter;

      counter++;
    }

    assertLogContent(writeAheadLog, writtenRecords.subList(firstSegmentIndex + 1, writtenRecords.size()));
    Assert.assertNull(writeAheadLog.getLastCheckpoint());
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(firstSegmentIndex).getLsn()));

    verify(paginatedStorage).scheduleCheckpoint();
    Assert.assertEquals(writeAheadLog.begin(), writtenRecords.get(firstSegmentIndex + 1).getLsn());
  }

  public void testLogTwoCheckPointTruncationOneLeft() throws Exception {
    writeAheadLog.close();

    OLocalPaginatedStorage paginatedStorage = mock(OLocalPaginatedStorage.class);
    when(paginatedStorage.getName()).thenReturn("WriteAheadLogTest");
    when(paginatedStorage.getStoragePath()).thenReturn(testDir.getAbsolutePath());

    writeAheadLog = new OWriteAheadLog(2, -1, 2 * OWALPage.PAGE_SIZE, 4 * OWALPage.PAGE_SIZE, paginatedStorage);

    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();
    long seed = System.currentTimeMillis();
    System.out.println("testLogTwoCheckPointTruncationOneLeft seed " + seed);

    Random rnd = new Random(seed);

    OWALRecord walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    int firstSegmentIndex = -1;
    int counter = 1;

    long logSize = writeAheadLog.size() + 1;
    long prevLogSize = 0;

    while (logSize > prevLogSize) {
      int contentSize = rnd.nextInt(OWALPage.PAGE_SIZE - 128) + 128;
      walRecord = new TestRecord(contentSize);

      writeAheadLog.logRecord(walRecord);
      writtenRecords.add(walRecord);

      prevLogSize = logSize;
      logSize = writeAheadLog.size();

      if (firstSegmentIndex < 0 && logSize > 2 * OWALPage.PAGE_SIZE)
        firstSegmentIndex = counter;

      counter++;
    }

    walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    assertLogContent(writeAheadLog, writtenRecords.subList(firstSegmentIndex + 1, writtenRecords.size()));
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(firstSegmentIndex).getLsn()));

    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), walRecord.getLsn());
    verify(paginatedStorage).scheduleCheckpoint();
    Assert.assertEquals(writeAheadLog.begin(), writtenRecords.get(firstSegmentIndex + 1).getLsn());
  }

  public void testLogThreeCheckPointTruncationAllDropped() throws Exception {
    writeAheadLog.close();

    OLocalPaginatedStorage paginatedStorage = mock(OLocalPaginatedStorage.class);
    when(paginatedStorage.getName()).thenReturn("WriteAheadLogTest");
    when(paginatedStorage.getStoragePath()).thenReturn(testDir.getAbsolutePath());
    writeAheadLog = new OWriteAheadLog(2, -1, 2 * OWALPage.PAGE_SIZE, 4 * OWALPage.PAGE_SIZE, paginatedStorage);

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

    int firstSegmentIndex = -1;
    int counter = 3;

    long logSize = writeAheadLog.size() + 1;
    long prevLogSize = 0;

    while (logSize > prevLogSize) {
      int contentSize = rnd.nextInt(OWALPage.PAGE_SIZE - 128) + 128;
      walRecord = new TestRecord(contentSize);

      writeAheadLog.logRecord(walRecord);
      writtenRecords.add(walRecord);

      prevLogSize = logSize;
      logSize = writeAheadLog.size();

      if (firstSegmentIndex < 0 && logSize > 2 * OWALPage.PAGE_SIZE)
        firstSegmentIndex = counter;

      counter++;
    }

    assertLogContent(writeAheadLog, writtenRecords.subList(firstSegmentIndex + 1, writtenRecords.size()));
    Assert.assertNull(writeAheadLog.getLastCheckpoint());
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(firstSegmentIndex).getLsn()));

    verify(paginatedStorage).scheduleCheckpoint();
    Assert.assertEquals(writeAheadLog.begin(), writtenRecords.get(firstSegmentIndex + 1).getLsn());
  }

  public void testLogThreeCheckPointTruncationOneLeft() throws Exception {
    writeAheadLog.close();

    OLocalPaginatedStorage paginatedStorage = mock(OLocalPaginatedStorage.class);
    when(paginatedStorage.getName()).thenReturn("WriteAheadLogTest");
    when(paginatedStorage.getStoragePath()).thenReturn(testDir.getAbsolutePath());

    writeAheadLog = new OWriteAheadLog(2, -1, 2 * OWALPage.PAGE_SIZE, 4 * OWALPage.PAGE_SIZE, paginatedStorage);

    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();
    Random rnd = new Random();

    OWALRecord walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    int firstSegmentIndex = -1;
    int counter = 2;

    long logSize = writeAheadLog.size() + 1;
    long prevLogSize = 0;

    while (logSize > prevLogSize) {
      int contentSize = rnd.nextInt(OWALPage.PAGE_SIZE - 128) + 128;
      walRecord = new TestRecord(contentSize);

      writeAheadLog.logRecord(walRecord);
      writtenRecords.add(walRecord);

      prevLogSize = logSize;
      logSize = writeAheadLog.size();

      if (firstSegmentIndex < 0 && logSize > 2 * OWALPage.PAGE_SIZE)
        firstSegmentIndex = counter;

      counter++;
    }

    walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    assertLogContent(writeAheadLog, writtenRecords.subList(firstSegmentIndex + 1, writtenRecords.size()));
    Assert.assertEquals(walRecord.getLsn(), writeAheadLog.getLastCheckpoint());

    Assert.assertNull(writeAheadLog.read(writtenRecords.get(firstSegmentIndex).getLsn()));

    verify(paginatedStorage).scheduleCheckpoint();
    Assert.assertEquals(writeAheadLog.begin(), writtenRecords.get(firstSegmentIndex + 1).getLsn());
  }

  public void testPageIsBroken() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    OWALRecord walRecord = new TestRecord(ONE_KB);
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    long logSize = writeAheadLog.size();

    walRecord = new TestRecord(OWALPage.PAGE_SIZE);
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(ONE_KB);
    OLogSequenceNumber lsn = writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    writeAheadLog.close();

    RandomAccessFile rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "rw");
    rndFile.seek(lsn.getPosition());
    int bt = rndFile.read();
    rndFile.seek(lsn.getPosition());
    rndFile.write(bt + 1);
    rndFile.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.size(), logSize);
    assertLogContent(writeAheadLog, writtenRecords.subList(0, 1));
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(1).getLsn()));
  }

  public void testPageIsBrokenOnOtherSegment() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    long seed = System.currentTimeMillis();
    System.out.println("testPageIsBrokenOnOtherSegment seed " + seed);
    Random random = new Random(seed);

    OWALRecord walRecord = new TestRecord(ONE_KB);
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    long logSize = writeAheadLog.size();

    walRecord = new TestRecord(OWALPage.PAGE_SIZE);
    writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(ONE_KB);
    OLogSequenceNumber lsn = writeAheadLog.logRecord(walRecord);
    writtenRecords.add(walRecord);

    writeAheadLog.close();

    RandomAccessFile rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "rw");
    rndFile.seek(lsn.getPosition());
    int bt = rndFile.read();
    rndFile.seek(lsn.getPosition());
    rndFile.write(bt + 1);
    rndFile.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.size(), logSize);
    assertLogContent(writeAheadLog, writtenRecords.subList(0, 1));
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(1).getLsn()));
  }

  private void assertLogContent(OWriteAheadLog writeAheadLog, List<? extends OWALRecord> writtenRecords) throws Exception {
    Iterator<? extends OWALRecord> iterator = writtenRecords.iterator();

    OWALRecord writtenRecord = iterator.next();
    OWALRecord readRecord = writeAheadLog.read(writtenRecord.getLsn());

    Assert.assertEquals(writtenRecord, readRecord);
    while (iterator.hasNext()) {
      writtenRecord = iterator.next();
      OLogSequenceNumber lsn = writeAheadLog.next(readRecord.getLsn());

      Assert.assertEquals(lsn, writtenRecord.getLsn());
      readRecord = writeAheadLog.read(lsn);

      Assert.assertEquals(writtenRecord, readRecord);
    }

    Assert.assertNull(writeAheadLog.next(readRecord.getLsn()));
  }

  public static final class TestRecord implements OWALRecord {
    private OLogSequenceNumber lsn;
    private byte[]             data;

    public TestRecord() {
    }

    private TestRecord(int size) {
      Random random = new Random();
      data = new byte[size - OIntegerSerializer.INT_SIZE - (OIntegerSerializer.INT_SIZE + 3)];
      random.nextBytes(data);
    }

    @Override
    public int toStream(byte[] content, int offset) {
      OIntegerSerializer.INSTANCE.serializeNative(data.length, content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      System.arraycopy(data, 0, content, offset, data.length);
      offset += data.length;

      return offset;
    }

    @Override
    public int fromStream(byte[] content, int offset) {
      int size = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      data = new byte[size];
      System.arraycopy(content, offset, data, 0, data.length);
      offset += size;

      return offset;
    }

    @Override
    public int serializedSize() {
      return OIntegerSerializer.INT_SIZE + data.length;
    }

    @Override
    public boolean isUpdateMasterRecord() {
      return false;
    }

    @Override
    public OLogSequenceNumber getLsn() {
      return lsn;
    }

    @Override
    public void setLsn(OLogSequenceNumber lsn) {
      this.lsn = lsn;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      TestRecord that = (TestRecord) o;

      if (!Arrays.equals(data, that.data))
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(data);
    }
  }

}
