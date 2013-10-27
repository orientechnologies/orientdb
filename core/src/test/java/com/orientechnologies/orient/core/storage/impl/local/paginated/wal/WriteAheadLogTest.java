package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

import org.testng.Assert;
import org.testng.annotations.*;

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
    return createWAL(2, OWALPage.PAGE_SIZE * 4);
  }

  private OWriteAheadLog createWAL(int maxPagesCacheSize, int maxSegmentSize) throws IOException {
    OLocalPaginatedStorage paginatedStorage = mock(OLocalPaginatedStorage.class);
    when(paginatedStorage.getName()).thenReturn("WriteAheadLogTest");
    when(paginatedStorage.getStoragePath()).thenReturn(testDir.getAbsolutePath());

    return new OWriteAheadLog(maxPagesCacheSize, -1, maxSegmentSize, 100L * 1024L * 1024L * 1024L, paginatedStorage);
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

  public void testLogCheckpoints() throws Exception {
    OLogSequenceNumber checkPointOneLSN = writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFullCheckpointEnd();

    OLogSequenceNumber checkPointTwoLSN = writeAheadLog.logFullCheckpointStart();
    writeAheadLog.logFullCheckpointEnd();

    OLogSequenceNumber checkPointThreeLSN = writeAheadLog.logFuzzyCheckPointStart();
    OLogSequenceNumber end = writeAheadLog.logFuzzyCheckPointEnd();

    OFuzzyCheckpointStartRecord fuzzyCheckpointStartRecordOne = (OFuzzyCheckpointStartRecord) writeAheadLog.read(checkPointOneLSN);
    OFullCheckpointStartRecord checkpointStartRecordTwo = (OFullCheckpointStartRecord) writeAheadLog.read(checkPointTwoLSN);
    OFuzzyCheckpointStartRecord fuzzyCheckpointStartRecordThree = (OFuzzyCheckpointStartRecord) writeAheadLog
        .read(checkPointThreeLSN);

    Assert.assertNull(fuzzyCheckpointStartRecordOne.getPreviousCheckpoint());
    Assert.assertEquals(checkpointStartRecordTwo.getPreviousCheckpoint(), checkPointOneLSN);
    Assert.assertEquals(fuzzyCheckpointStartRecordThree.getPreviousCheckpoint(), checkPointTwoLSN);

    Assert.assertEquals(writeAheadLog.end(), end);
  }

  public void testWriteSingleRecord() throws Exception {
    Assert.assertNull(writeAheadLog.end());

    TestRecord writtenRecord = new TestRecord(30, false);
    writeAheadLog.log(writtenRecord);

    OWALRecord walRecord = writeAheadLog.read(writeAheadLog.begin());
    Assert.assertTrue(walRecord instanceof TestRecord);

    TestRecord testRecord = (TestRecord) walRecord;
    Assert.assertEquals(testRecord, writtenRecord);

    Assert.assertNull(writeAheadLog.next(walRecord.getLsn()));

    writeAheadLog.close();
    writeAheadLog = createWAL();

    walRecord = writeAheadLog.read(writeAheadLog.begin());
    Assert.assertEquals(walRecord.getLsn(), writeAheadLog.begin());
    Assert.assertTrue(walRecord instanceof TestRecord);

    testRecord = (TestRecord) walRecord;
    Assert.assertEquals(testRecord, writtenRecord);

    Assert.assertNull(writeAheadLog.next(writeAheadLog.begin()));
  }

  public void testFirstMasterRecordUpdate() throws Exception {
    TestRecord writtenRecord = new TestRecord(30, false);

    writeAheadLog.log(writtenRecord);
    OLogSequenceNumber masterLSN = writeAheadLog.logFuzzyCheckPointStart();

    OLogSequenceNumber end = writeAheadLog.logFuzzyCheckPointEnd();

    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), masterLSN);
    Assert.assertEquals(writeAheadLog.end(), end);

    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), masterLSN);
    Assert.assertEquals(writeAheadLog.end(), end);
  }

  public void testSecondMasterRecordUpdate() throws Exception {
    writeAheadLog.log(new TestRecord(30, false));

    writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.log(new TestRecord(30, false));

    OLogSequenceNumber checkpointLSN = writeAheadLog.logFuzzyCheckPointStart();
    OLogSequenceNumber end = writeAheadLog.logFuzzyCheckPointEnd();

    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), checkpointLSN);
    Assert.assertEquals(writeAheadLog.end(), end);
    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), checkpointLSN);
    Assert.assertEquals(writeAheadLog.end(), end);
  }

  public void testThirdMasterRecordUpdate() throws Exception {
    writeAheadLog.log(new TestRecord(30, false));

    writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.log(new TestRecord(30, false));

    writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.log(new TestRecord(30, false));

    OLogSequenceNumber checkpointLSN = writeAheadLog.logFuzzyCheckPointStart();
    OLogSequenceNumber end = writeAheadLog.logFuzzyCheckPointEnd();

    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), checkpointLSN);
    Assert.assertEquals(writeAheadLog.end(), end);
    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), checkpointLSN);
    Assert.assertEquals(writeAheadLog.end(), end);
  }

  public void testWriteMultipleRecordsWithDifferentSizes() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    Assert.assertEquals(writeAheadLog.size(), 0);

    long logSize = 0;
    int contentSize;
    // first page
    contentSize = ONE_KB;
    OWALRecord walRecord = new TestRecord(contentSize, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);
    logSize += OWALPage.RECORDS_OFFSET + contentSize;
    Assert.assertEquals(writeAheadLog.size(), logSize);

    contentSize = ONE_KB;
    walRecord = new TestRecord(contentSize, false);
    writeAheadLog.log(walRecord);
    logSize += contentSize;
    Assert.assertEquals(writeAheadLog.size(), logSize);
    writtenRecords.add(walRecord);

    contentSize = OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET - 2048 - OWALPage.MIN_RECORD_SIZE + 1;
    walRecord = new TestRecord(contentSize, false);
    writeAheadLog.log(walRecord);

    logSize += contentSize;
    Assert.assertEquals(writeAheadLog.size(), logSize);
    writtenRecords.add(walRecord);

    // second page
    contentSize = ONE_KB;
    walRecord = new TestRecord(contentSize, false);
    writeAheadLog.log(walRecord);
    logSize += OWALPage.MIN_RECORD_SIZE - 1 + OWALPage.RECORDS_OFFSET + contentSize;
    Assert.assertEquals(writeAheadLog.size(), logSize);
    writtenRecords.add(walRecord);

    contentSize = ONE_KB;
    walRecord = new TestRecord(contentSize, false);
    writeAheadLog.log(walRecord);
    logSize += contentSize;
    Assert.assertEquals(writeAheadLog.size(), logSize);
    writtenRecords.add(walRecord);

    contentSize = OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET - 2048 - OWALPage.MIN_RECORD_SIZE;
    walRecord = new TestRecord(contentSize, false);
    writeAheadLog.log(walRecord);
    logSize += contentSize;
    Assert.assertEquals(writeAheadLog.size(), logSize);
    writtenRecords.add(walRecord);

    // third page
    contentSize = ONE_KB;
    walRecord = new TestRecord(contentSize, false);
    writeAheadLog.log(walRecord);
    logSize += contentSize - 1 + OWALPage.MIN_RECORD_SIZE + OWALPage.RECORDS_OFFSET;
    Assert.assertEquals(writeAheadLog.size(), logSize);
    writtenRecords.add(walRecord);

    contentSize = ONE_KB;
    walRecord = new TestRecord(contentSize, false);
    writeAheadLog.log(walRecord);
    logSize += contentSize;
    Assert.assertEquals(writeAheadLog.size(), logSize);
    writtenRecords.add(walRecord);

    contentSize = OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET - 2047 - OWALPage.MIN_RECORD_SIZE;
    walRecord = new TestRecord(contentSize, false);
    writeAheadLog.log(walRecord);
    logSize += contentSize;
    writtenRecords.add(walRecord);

    // fourth page
    contentSize = ONE_KB;
    walRecord = new TestRecord(contentSize, false);
    writeAheadLog.log(walRecord);
    logSize += contentSize - 1 + OWALPage.MIN_RECORD_SIZE + OWALPage.RECORDS_OFFSET;
    Assert.assertEquals(writeAheadLog.size(), logSize);
    writtenRecords.add(walRecord);

    contentSize = ONE_KB;
    walRecord = new TestRecord(contentSize, false);
    writeAheadLog.log(walRecord);
    logSize += contentSize;
    Assert.assertEquals(writeAheadLog.size(), logSize);
    writtenRecords.add(walRecord);

    contentSize = OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET - 2047;
    walRecord = new TestRecord(contentSize, false);
    writeAheadLog.log(walRecord);
    logSize += contentSize;
    Assert.assertEquals(writeAheadLog.size(), logSize);
    writtenRecords.add(walRecord);

    // fifth page
    contentSize = ONE_KB;
    walRecord = new TestRecord(contentSize, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);

    logSize += contentSize + OWALPage.RECORDS_OFFSET;
    Assert.assertEquals(writeAheadLog.size(), logSize);
    writtenRecords.add(walRecord);

    assertLogContent(writeAheadLog, writtenRecords);

    Assert.assertEquals(writeAheadLog.end(), end);
    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.size(), logSize);
    Assert.assertEquals(writeAheadLog.end(), end);

    assertLogContent(writeAheadLog, writtenRecords);
  }

  public void testWriteMultipleRecordsWithDifferentSizeAfterCloseOne() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    // first page
    OWALRecord walRecord = new TestRecord(ONE_KB, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(ONE_KB, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    long seed = System.currentTimeMillis();
    System.out.println("seed of testWriteMultipleRecordsWithDifferentSizeAfterCloseOne " + seed);
    Random random = new Random(seed);
    writeAheadLog = createWAL();

    OLogSequenceNumber end = null;

    for (int writtenSize = 0; writtenSize < 4 * OWALPage.PAGE_SIZE;) {
      int contentSize = random.nextInt(2 * OWALPage.PAGE_SIZE - 1) + 1;
      walRecord = new TestRecord(contentSize, false);

      end = writeAheadLog.log(walRecord);
      writtenRecords.add(walRecord);

      writtenSize += contentSize;
    }

    Assert.assertEquals(writeAheadLog.end(), end);
    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    assertLogContent(writeAheadLog, writtenRecords);
  }

  public void testWriteMultipleRecordsWithDifferentSizeAfterCloseTwo() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    // first page
    OWALRecord walRecord = new TestRecord(ONE_KB, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(ONE_KB, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET - 2048 - OWALPage.MIN_RECORD_SIZE + 1, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    long seed = System.currentTimeMillis();
    System.out.println("seed of testWriteMultipleRecordsWithDifferentSizeAfterCloseTwo " + seed);
    Random random = new Random(seed);
    writeAheadLog = createWAL();

    OLogSequenceNumber end = null;
    for (int writtenSize = 0; writtenSize < 4 * OWALPage.PAGE_SIZE;) {
      int contentSize = random.nextInt(2 * OWALPage.PAGE_SIZE - 1) + 1;
      walRecord = new TestRecord(contentSize, false);

      end = writeAheadLog.log(walRecord);
      writtenRecords.add(walRecord);

      writtenSize += contentSize;
    }

    assertLogContent(writeAheadLog, writtenRecords);

    Assert.assertEquals(writeAheadLog.end(), end);
    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.end(), end);

    assertLogContent(writeAheadLog, writtenRecords);
  }

  public void testWriteMultipleRecordsWithDifferentSizeAfterCloseThree() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    // first page
    OWALRecord walRecord = new TestRecord(ONE_KB, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(ONE_KB, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET - 2048 - OWALPage.MIN_RECORD_SIZE, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    long seed = System.currentTimeMillis();
    System.out.println("seed of testWriteMultipleRecordsWithDifferentSizeAfterCloseThree " + seed);
    Random random = new Random(seed);
    writeAheadLog = createWAL();

    OLogSequenceNumber end = null;

    for (int writtenSize = 0; writtenSize < 4 * OWALPage.PAGE_SIZE;) {
      int contentSize = random.nextInt(2 * OWALPage.PAGE_SIZE - 1) + 1;
      walRecord = new TestRecord(contentSize, false);

      end = writeAheadLog.log(walRecord);
      writtenRecords.add(walRecord);

      writtenSize += contentSize;
    }

    assertLogContent(writeAheadLog, writtenRecords);

    Assert.assertEquals(writeAheadLog.end(), end);
    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.end(), end);

    assertLogContent(writeAheadLog, writtenRecords);
  }

  public void testWriteMultipleRecordsWithDifferentSizeAfterCloseFour() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    // first page
    OWALRecord walRecord = new TestRecord(ONE_KB, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(ONE_KB, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET - 2048, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    long seed = System.currentTimeMillis();
    System.out.println("seed of testWriteMultipleRecordsWithDifferentSizeAfterCloseFour " + seed);
    Random random = new Random(seed);
    writeAheadLog = createWAL();

    OLogSequenceNumber end = null;
    for (int writtenSize = 0; writtenSize < 4 * OWALPage.PAGE_SIZE;) {
      int contentSize = random.nextInt(2 * OWALPage.PAGE_SIZE - 1) + 1;
      walRecord = new TestRecord(contentSize, false);

      end = writeAheadLog.log(walRecord);
      writtenRecords.add(walRecord);

      writtenSize += contentSize;
    }

    Assert.assertEquals(writeAheadLog.end(), end);
    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.end(), end);

    assertLogContent(writeAheadLog, writtenRecords);
  }

  public void testWriteMultipleRandomRecords() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    long seed = System.currentTimeMillis();
    System.out.println("seed of testWriteMultipleRecordsWithDifferentSizeAfterCloseFour " + seed);
    Random random = new Random(seed);

    for (int writtenSize = 0; writtenSize < 16 * OWALPage.PAGE_SIZE;) {
      int contentSize = random.nextInt(2 * OWALPage.PAGE_SIZE - 1) + 1;
      OWALRecord walRecord = new TestRecord(contentSize, false);

      writeAheadLog.log(walRecord);
      writtenRecords.add(walRecord);

      writtenSize += contentSize;
    }

    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    writeAheadLog = createWAL();

    assertLogContent(writeAheadLog, writtenRecords);

    OLogSequenceNumber end = null;
    for (int writtenSize = 0; writtenSize < 16 * OWALPage.PAGE_SIZE;) {
      int contentSize = random.nextInt(2 * OWALPage.PAGE_SIZE - 1) + 1;
      OWALRecord walRecord = new TestRecord(contentSize, false);

      end = writeAheadLog.log(walRecord);
      writtenRecords.add(walRecord);

      writtenSize += contentSize;
    }

    Assert.assertEquals(writeAheadLog.end(), end);
    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    assertLogContent(writeAheadLog, writtenRecords);

  }

  public void testFlushedLSNOnePage() throws Exception {
    OWALRecord walRecord = new TestRecord(ONE_KB, false);
    writeAheadLog.log(walRecord);

    walRecord = new TestRecord(ONE_KB, false);
    writeAheadLog.log(walRecord);

    Assert.assertNull(writeAheadLog.getFlushedLSN());

    writeAheadLog.flush();

    OLogSequenceNumber end = writeAheadLog.end();
    Assert.assertEquals(writeAheadLog.getFlushedLSN(), walRecord.getLsn());

    Assert.assertEquals(writeAheadLog.end(), end);
    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.end(), end);

    Assert.assertEquals(writeAheadLog.getFlushedLSN(), walRecord.getLsn());
  }

  public void testFlushedLSNOnePageWithLessThanMinRecordSpace() throws Exception {
    OWALRecord walRecord = new TestRecord(ONE_KB, false);
    writeAheadLog.log(walRecord);

    walRecord = new TestRecord(ONE_KB, false);
    writeAheadLog.log(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET - 2048 - OWALPage.MIN_RECORD_SIZE + 1, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);

    Assert.assertNull(writeAheadLog.getFlushedLSN());

    writeAheadLog.flush();

    Assert.assertEquals(writeAheadLog.getFlushedLSN(), walRecord.getLsn());
    Assert.assertEquals(writeAheadLog.end(), end);

    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.end(), end);

    Assert.assertEquals(writeAheadLog.getFlushedLSN(), walRecord.getLsn());
  }

  public void testFlushedLSNOnePageWithMinRecordSpace() throws Exception {
    OWALRecord walRecord = new TestRecord(ONE_KB, false);
    writeAheadLog.log(walRecord);

    walRecord = new TestRecord(ONE_KB, false);
    writeAheadLog.log(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET - 2048 - OWALPage.MIN_RECORD_SIZE, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);

    Assert.assertNull(writeAheadLog.getFlushedLSN());

    writeAheadLog.flush();

    Assert.assertEquals(writeAheadLog.getFlushedLSN(), walRecord.getLsn());

    Assert.assertEquals(writeAheadLog.end(), end);
    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLSN(), walRecord.getLsn());
  }

  public void testFlushedLSNOnePageWithNoSpace() throws Exception {
    OWALRecord walRecord = new TestRecord(ONE_KB, false);
    writeAheadLog.log(walRecord);

    walRecord = new TestRecord(ONE_KB, false);
    writeAheadLog.log(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET - 2048, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);

    Assert.assertNull(writeAheadLog.getFlushedLSN());

    writeAheadLog.flush();

    Assert.assertEquals(writeAheadLog.getFlushedLSN(), walRecord.getLsn());

    Assert.assertEquals(writeAheadLog.end(), end);
    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLSN(), walRecord.getLsn());
  }

  public void testFlushedLSNTwoPagesAndThenTwo() throws Exception {
    OWALRecord walRecord = null;
    for (int i = 0; i < 2; i++) {
      walRecord = new TestRecord(ONE_KB, false);
      writeAheadLog.log(walRecord);

      walRecord = new TestRecord(ONE_KB, false);
      writeAheadLog.log(walRecord);

      walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET - 2048, false);
      writeAheadLog.log(walRecord);
    }

    Assert.assertNull(writeAheadLog.getFlushedLSN());

    writeAheadLog.flush();

    OLogSequenceNumber end = null;
    for (int i = 0; i < 2; i++) {
      walRecord = new TestRecord(ONE_KB, false);
      writeAheadLog.log(walRecord);

      walRecord = new TestRecord(ONE_KB, false);
      writeAheadLog.log(walRecord);

      walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET - 2048, false);
      end = writeAheadLog.log(walRecord);
    }

    writeAheadLog.flush();
    Assert.assertEquals(writeAheadLog.getFlushedLSN(), walRecord.getLsn());
    Assert.assertEquals(writeAheadLog.end(), end);
    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.end(), end);

    Assert.assertEquals(writeAheadLog.getFlushedLSN(), walRecord.getLsn());
    Assert.assertEquals(writeAheadLog.end(), end);
  }

  public void testFlushedLSNTwoPagesOneWithTrail() throws Exception {
    OWALRecord walRecord = new TestRecord(ONE_KB, false);
    writeAheadLog.log(walRecord);

    walRecord = new TestRecord(ONE_KB, false);
    writeAheadLog.log(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET - 2048 - OWALPage.MIN_RECORD_SIZE, false);
    writeAheadLog.log(walRecord);

    walRecord = new TestRecord(ONE_KB, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);

    Assert.assertNull(writeAheadLog.getFlushedLSN());

    writeAheadLog.flush();
    Assert.assertEquals(writeAheadLog.getFlushedLSN(), walRecord.getLsn());

    Assert.assertEquals(writeAheadLog.end(), end);
    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
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
      walRecord = new TestRecord(contentSize, false);
      writeAheadLog.log(walRecord);

      writtenContent += contentSize;
    }

    int contentSize = random.nextInt(OWALPage.PAGE_SIZE - 1) + 1;
    walRecord = new TestRecord(contentSize, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);

    writeAheadLog.flush();
    Assert.assertEquals(writeAheadLog.getFlushedLSN(), walRecord.getLsn());

    Assert.assertEquals(writeAheadLog.end(), end);
    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLSN(), walRecord.getLsn());
  }

  public void testFirstMasterRecordIsBrokenSingleRecord() throws Exception {
    writeAheadLog.log(new TestRecord(30, false));

    writeAheadLog.logFuzzyCheckPointStart();
    OLogSequenceNumber end = writeAheadLog.logFuzzyCheckPointEnd();

    Assert.assertEquals(writeAheadLog.end(), end);
    writeAheadLog.close();

    RandomAccessFile mrFile = new RandomAccessFile(new File(writeAheadLog.getWalLocation(), "WriteAheadLogTest.wmr"), "rw");
    mrFile.seek(OIntegerSerializer.INT_SIZE + 1);

    int bt = mrFile.read();
    mrFile.seek(OIntegerSerializer.INT_SIZE + 1);
    mrFile.write(bt + 1);
    mrFile.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertNull(writeAheadLog.getLastCheckpoint());
  }

  public void testSecondMasterRecordIsBroken() throws Exception {
    writeAheadLog.log(new TestRecord(30, false));

    OLogSequenceNumber checkPointLSN = writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.log(new TestRecord(OWALPage.PAGE_SIZE, false));

    writeAheadLog.logFuzzyCheckPointStart();
    OLogSequenceNumber end = writeAheadLog.logFuzzyCheckPointEnd();

    Assert.assertEquals(writeAheadLog.end(), end);
    writeAheadLog.close();

    RandomAccessFile mrFile = new RandomAccessFile(new File(writeAheadLog.getWalLocation(), "WriteAheadLogTest.wmr"), "rw");
    mrFile.seek(3 * OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE);

    int bt = mrFile.read();
    mrFile.seek(3 * OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE);
    mrFile.write(bt + 1);
    mrFile.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), checkPointLSN);
  }

  public void testFirstMasterRecordIsBrokenThreeCheckpoints() throws Exception {
    writeAheadLog.log(new TestRecord(30, false));

    writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.log(new TestRecord(OWALPage.PAGE_SIZE, false));

    OLogSequenceNumber checkPointLSN = writeAheadLog.logFuzzyCheckPointStart();
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.log(new TestRecord(OWALPage.PAGE_SIZE, false));

    writeAheadLog.logFuzzyCheckPointStart();
    OLogSequenceNumber end = writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.close();

    RandomAccessFile mrFile = new RandomAccessFile(new File(writeAheadLog.getWalLocation(), "WriteAheadLogTest.wmr"), "rw");
    mrFile.seek(0);

    int bt = mrFile.read();
    mrFile.seek(0);
    mrFile.write(bt + 1);
    mrFile.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), checkPointLSN);
  }

  public void testWriteMultipleRecords() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    final int recordsToWrite = 2048;
    OLogSequenceNumber end = null;
    for (int i = 0; i < recordsToWrite; i++) {
      TestRecord setPageDataRecord = new TestRecord(30, false);
      writtenRecords.add(setPageDataRecord);

      end = writeAheadLog.log(setPageDataRecord);
    }

    assertLogContent(writeAheadLog, writtenRecords);
    assertLogContent(writeAheadLog, writtenRecords.subList(writtenRecords.size() / 2, writtenRecords.size()));

    Assert.assertEquals(writeAheadLog.end(), end);
    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.end(), end);
    assertLogContent(writeAheadLog, writtenRecords);
    assertLogContent(writeAheadLog, writtenRecords.subList(writtenRecords.size() / 2, writtenRecords.size()));
  }

  public void testAppendMultipleRecordsAfterClose() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    OLogSequenceNumber end = null;
    final int recordsToWrite = 1;
    for (int i = 0; i < recordsToWrite; i++) {
      TestRecord testRecord = new TestRecord(30, false);
      writtenRecords.add(testRecord);

      end = writeAheadLog.log(testRecord);
    }

    Assert.assertEquals(writeAheadLog.end(), end);
    writeAheadLog.close();
    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.end(), end);

    for (int i = 0; i < recordsToWrite; i++) {
      TestRecord testRecord = new TestRecord(30, false);
      writtenRecords.add(testRecord);

      end = writeAheadLog.log(testRecord);
    }

    assertLogContent(writeAheadLog, writtenRecords);
    assertLogContent(writeAheadLog, writtenRecords.subList(writtenRecords.size() / 2, writtenRecords.size()));

    Assert.assertEquals(writeAheadLog.end(), end);
    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.end(), end);

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

    OLogSequenceNumber end = null;
    while (logSize > prevLogSize) {
      int contentSize = rnd.nextInt(OWALPage.PAGE_SIZE - 128) + 128;
      OWALRecord walRecord = new TestRecord(contentSize, false);

      end = writeAheadLog.log(walRecord);
      writtenRecords.add(walRecord);

      prevLogSize = logSize;
      logSize = writeAheadLog.size();

      if (firstSegmentIndex < 0 && logSize > 2 * OWALPage.PAGE_SIZE)
        firstSegmentIndex = counter;

      counter++;
    }

    Assert.assertEquals(writeAheadLog.end(), end);
    assertLogContent(writeAheadLog, writtenRecords.subList(firstSegmentIndex + 1, writtenRecords.size()));

    Assert.assertNull(writeAheadLog.read(writtenRecords.get(firstSegmentIndex).getLsn()));
  }

  public void testLogOneCheckPointTruncation() throws Exception {
    writeAheadLog.close();

    OLocalPaginatedStorage paginatedStorage = mock(OLocalPaginatedStorage.class);
    when(paginatedStorage.getName()).thenReturn("WriteAheadLogTest");
    when(paginatedStorage.getStoragePath()).thenReturn(testDir.getAbsolutePath());

    writeAheadLog = new OWriteAheadLog(2, -1, 2 * OWALPage.PAGE_SIZE, 4 * OWALPage.PAGE_SIZE, paginatedStorage);

    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    OWALRecord walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.log(walRecord);

    writtenRecords.add(walRecord);

    long seed = System.currentTimeMillis();
    System.out.println("testLogOneCheckPointTruncation seed " + seed);

    Random rnd = new Random(seed);

    int firstSegmentIndex = -1;
    int counter = 1;

    long logSize = writeAheadLog.size() + 1;
    long prevLogSize = 0;

    OLogSequenceNumber end = null;
    while (logSize > prevLogSize) {
      int contentSize = rnd.nextInt(OWALPage.PAGE_SIZE - 128) + 128;
      walRecord = new TestRecord(contentSize, false);

      end = writeAheadLog.log(walRecord);
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

    Assert.assertEquals(writeAheadLog.end(), end);
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
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    int firstSegmentIndex = -1;
    int counter = 2;

    long logSize = writeAheadLog.size() + 1;
    long prevLogSize = 0;

    OLogSequenceNumber end = null;
    while (logSize > prevLogSize) {
      int contentSize = rnd.nextInt(OWALPage.PAGE_SIZE - 128) + 128;
      walRecord = new TestRecord(contentSize, false);

      end = writeAheadLog.log(walRecord);
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

    Assert.assertEquals(writeAheadLog.end(), end);
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
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    int firstSegmentIndex = -1;
    int counter = 1;

    long logSize = writeAheadLog.size() + 1;
    long prevLogSize = 0;

    while (logSize > prevLogSize) {
      int contentSize = rnd.nextInt(OWALPage.PAGE_SIZE - 128) + 128;
      walRecord = new TestRecord(contentSize, false);

      writeAheadLog.log(walRecord);
      writtenRecords.add(walRecord);

      prevLogSize = logSize;
      logSize = writeAheadLog.size();

      if (firstSegmentIndex < 0 && logSize > 2 * OWALPage.PAGE_SIZE)
        firstSegmentIndex = counter;

      counter++;
    }

    walRecord = new OFuzzyCheckpointStartRecord();
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    assertLogContent(writeAheadLog, writtenRecords.subList(firstSegmentIndex + 1, writtenRecords.size()));
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(firstSegmentIndex).getLsn()));

    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), walRecord.getLsn());
    Assert.assertEquals(writeAheadLog.begin(), writtenRecords.get(firstSegmentIndex + 1).getLsn());
    Assert.assertEquals(writeAheadLog.end(), end);
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
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    int firstSegmentIndex = -1;
    int counter = 3;

    long logSize = writeAheadLog.size() + 1;
    long prevLogSize = 0;

    OLogSequenceNumber end = null;
    while (logSize > prevLogSize) {
      int contentSize = rnd.nextInt(OWALPage.PAGE_SIZE - 128) + 128;
      walRecord = new TestRecord(contentSize, false);

      end = writeAheadLog.log(walRecord);
      writtenRecords.add(walRecord);

      prevLogSize = logSize;
      logSize = writeAheadLog.size();

      if (firstSegmentIndex < 0 && logSize > 2 * OWALPage.PAGE_SIZE)
        firstSegmentIndex = counter;

      counter++;
    }

    Assert.assertEquals(writeAheadLog.end(), end);

    assertLogContent(writeAheadLog, writtenRecords.subList(firstSegmentIndex + 1, writtenRecords.size()));
    Assert.assertNull(writeAheadLog.getLastCheckpoint());
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(firstSegmentIndex).getLsn()));

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
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new OFuzzyCheckpointStartRecord();
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    int firstSegmentIndex = -1;
    int counter = 2;

    long logSize = writeAheadLog.size() + 1;
    long prevLogSize = 0;

    while (logSize > prevLogSize) {
      int contentSize = rnd.nextInt(OWALPage.PAGE_SIZE - 128) + 128;
      walRecord = new TestRecord(contentSize, false);

      writeAheadLog.log(walRecord);
      writtenRecords.add(walRecord);

      prevLogSize = logSize;
      logSize = writeAheadLog.size();

      if (firstSegmentIndex < 0 && logSize > 2 * OWALPage.PAGE_SIZE)
        firstSegmentIndex = counter;

      counter++;
    }

    walRecord = new OFuzzyCheckpointStartRecord();
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    assertLogContent(writeAheadLog, writtenRecords.subList(firstSegmentIndex + 1, writtenRecords.size()));
    Assert.assertEquals(walRecord.getLsn(), writeAheadLog.getLastCheckpoint());

    Assert.assertNull(writeAheadLog.read(writtenRecords.get(firstSegmentIndex).getLsn()));

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.begin(), writtenRecords.get(firstSegmentIndex + 1).getLsn());
  }

  public void testPageIsBroken() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    OWALRecord walRecord = new TestRecord(ONE_KB, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    long logSize = writeAheadLog.size();

    walRecord = new TestRecord(OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(ONE_KB, false);
    OLogSequenceNumber lsn = writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    writeAheadLog.close();

    RandomAccessFile rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "rw");
    rndFile.seek(lsn.getPosition());
    int bt = rndFile.read();
    rndFile.seek(lsn.getPosition());
    rndFile.write(bt + 1);
    rndFile.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.size(), logSize);
    assertLogContent(writeAheadLog, writtenRecords.subList(0, 1));
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(1).getLsn()));
  }

  public void testPageIsBrokenOnOtherSegment() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    OWALRecord walRecord = new TestRecord(ONE_KB, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    long logSize = writeAheadLog.size();

    walRecord = new TestRecord(OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(ONE_KB, false);
    OLogSequenceNumber lsn = writeAheadLog.log(walRecord);
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
    Assert.assertEquals(writeAheadLog.end(), end);
    assertLogContent(writeAheadLog, writtenRecords.subList(0, 1));
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(1).getLsn()));
  }

  public void testPageIsBrokenThreeSegmentsOneRecordIsTwoPageWide() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    OWALRecord walRecord = new TestRecord(ONE_KB, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    long logSize = writeAheadLog.size();

    walRecord = new TestRecord(2 * OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(ONE_KB, false);
    OLogSequenceNumber lsn = writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    writeAheadLog.close();

    RandomAccessFile rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "rw");
    rndFile.seek(lsn.getPosition());
    int bt = rndFile.read();
    rndFile.seek(lsn.getPosition());
    rndFile.write(bt + 1);
    rndFile.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.size(), logSize);
    assertLogContent(writeAheadLog, writtenRecords.subList(0, 1));
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(1).getLsn()));
  }

  public void testPageIsBrokenAndEmpty() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    OWALRecord walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    long logSize = writeAheadLog.size();

    walRecord = new TestRecord(3 * OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    writeAheadLog.close();

    RandomAccessFile rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "rw");
    rndFile.seek(writeAheadLog.size() - 1);
    int bt = rndFile.read();
    rndFile.seek(writeAheadLog.size() - 1);
    rndFile.write(bt + 1);
    rndFile.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.size(), logSize);
    assertLogContent(writeAheadLog, writtenRecords.subList(0, 1));
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(1).getLsn()));
  }

  public void testSecondPageWasTruncated() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    OWALRecord walRecord = new TestRecord(100, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    writeAheadLog.close();

    RandomAccessFile rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "rw");
    rndFile.setLength(OWALPage.PAGE_SIZE);
    rndFile.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    assertLogContent(writeAheadLog, writtenRecords.subList(0, 1));
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(1).getLsn()));
  }

  public void testThirdPageWasTruncated() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    OWALRecord walRecord = new TestRecord(100, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(2 * OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    writeAheadLog.close();

    RandomAccessFile rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "rw");
    rndFile.setLength(2 * OWALPage.PAGE_SIZE);
    rndFile.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    assertLogContent(writeAheadLog, writtenRecords.subList(0, 1));
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(1).getLsn()));
  }

  public void testThirdPageCRCWasIncorrect() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    OWALRecord walRecord = new TestRecord(100, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(2 * OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    writeAheadLog.close();

    RandomAccessFile rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "rw");
    rndFile.seek(2 * OWALPage.PAGE_SIZE);
    int bt = rndFile.read();
    rndFile.seek(2 * OWALPage.PAGE_SIZE);
    rndFile.write(bt + 1);
    rndFile.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    assertLogContent(writeAheadLog, writtenRecords.subList(0, 1));
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(1).getLsn()));
  }

  public void testFirstPageInFlushWasBroken() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    // first flush
    OWALRecord walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    // second flush
    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    writeAheadLog.close();

    RandomAccessFile rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "rw");
    rndFile.seek(2 * OWALPage.PAGE_SIZE);
    int bt = rndFile.read();
    rndFile.write(bt + 1);
    rndFile.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    assertLogContent(writeAheadLog, writtenRecords.subList(0, 2));
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(2).getLsn()));
  }

  public void testFirstInCompletePageInFlushWasBroken() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    // first flush
    OWALRecord walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET + 100, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    // second flush
    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET - 100, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    writeAheadLog.close();

    RandomAccessFile rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "rw");
    rndFile.seek(2 * OWALPage.PAGE_SIZE);
    int bt = rndFile.read();
    rndFile.write(bt + 1);
    rndFile.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    assertLogContent(writeAheadLog, writtenRecords.subList(0, 1));
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(1).getLsn()));
  }

  public void testMiddlePageInFlushWasBroken() throws Exception {
    writeAheadLog.close();
    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    // first flush
    OWALRecord walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);
    writeAheadLog.flush();

    // second flush
    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    writeAheadLog.close();

    RandomAccessFile rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "rw");
    rndFile.seek(3 * OWALPage.PAGE_SIZE);
    int bt = rndFile.read();
    rndFile.seek(3 * OWALPage.PAGE_SIZE);
    rndFile.write(bt + 1);
    rndFile.close();

    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    Assert.assertEquals(writeAheadLog.end(), end);
    assertLogContent(writeAheadLog, writtenRecords.subList(0, 3));
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(3).getLsn()));
  }

  public void testMiddleIncompletePageInFlushWasBroken() throws Exception {
    writeAheadLog.close();
    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    // first flush
    OWALRecord walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET + 100, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);
    writeAheadLog.flush();

    // second flush
    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET - 100, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    writeAheadLog.close();

    RandomAccessFile rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "rw");
    rndFile.seek(3 * OWALPage.PAGE_SIZE);
    int bt = rndFile.read();
    rndFile.write(bt + 1);
    rndFile.close();

    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    Assert.assertEquals(writeAheadLog.end(), end);
    assertLogContent(writeAheadLog, writtenRecords.subList(0, 2));
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(2).getLsn()));
  }

  public void testFirstPageWasNotFlushedFirstCase() throws Exception {
    writeAheadLog.close();
    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    // first flush
    OWALRecord walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET + 100, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);
    writeAheadLog.flush();

    RandomAccessFile rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "r");
    byte[] content = new byte[OWALPage.PAGE_SIZE];
    rndFile.seek(2 * OWALPage.PAGE_SIZE);
    rndFile.readFully(content);
    rndFile.close();

    // second flush
    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET - 100, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    writeAheadLog.close();

    rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "rw");
    rndFile.seek(2 * OWALPage.PAGE_SIZE);
    rndFile.write(content);
    rndFile.close();

    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    Assert.assertEquals(writeAheadLog.end(), end);
    assertLogContent(writeAheadLog, writtenRecords.subList(0, 2));
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(2).getLsn()));
  }

  public void testFirstPageWasNotFlushedSecondCase() throws Exception {
    writeAheadLog.close();
    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    // first flush
    OWALRecord walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET - 100, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    writeAheadLog.flush();

    RandomAccessFile rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "r");
    byte[] content = new byte[OWALPage.PAGE_SIZE];
    rndFile.seek(0);
    rndFile.readFully(content);
    rndFile.close();

    // second flush
    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET + 100, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET - 100, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    writeAheadLog.close();

    rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "rw");
    rndFile.seek(0);
    rndFile.write(content);
    rndFile.close();

    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    Assert.assertEquals(writeAheadLog.end(), end);
    assertLogContent(writeAheadLog, writtenRecords.subList(0, 1));
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(1).getLsn()));
  }

  public void testPageWasNotFullyWritten() throws Exception {
    writeAheadLog.close();
    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    // first flush
    OWALRecord walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);
    writeAheadLog.flush();

    // second flush
    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    writeAheadLog.close();

    RandomAccessFile rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "rw");
    rndFile.setLength(rndFile.length() - OWALPage.PAGE_SIZE / 2);
    rndFile.close();

    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    Assert.assertEquals(writeAheadLog.end(), end);
    assertLogContent(writeAheadLog, writtenRecords.subList(0, 4));
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(4).getLsn()));
  }

  public void testIncompletePageWasNotFullyWritten() throws Exception {
    writeAheadLog.close();
    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    // first flush
    OWALRecord walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);
    writeAheadLog.flush();

    // second flush
    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET + 100, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(OWALPage.PAGE_SIZE - OWALPage.RECORDS_OFFSET - 200, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    writeAheadLog.close();

    RandomAccessFile rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "rw");
    rndFile.setLength(rndFile.length() - OWALPage.PAGE_SIZE / 2);
    rndFile.close();

    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    Assert.assertEquals(writeAheadLog.end(), end);
    assertLogContent(writeAheadLog, writtenRecords.subList(0, 3));
    Assert.assertNull(writeAheadLog.read(writtenRecords.get(3).getLsn()));
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
    private boolean            updateMasterRecord;

    public TestRecord() {
    }

    public TestRecord(int size, boolean updateMasterRecord) {
      Random random = new Random();
      data = new byte[size - OIntegerSerializer.INT_SIZE - (OIntegerSerializer.INT_SIZE + 3) - 1];
      random.nextBytes(data);
      this.updateMasterRecord = updateMasterRecord;
    }

    @Override
    public int toStream(byte[] content, int offset) {
      content[offset] = updateMasterRecord ? (byte) 1 : 0;
      offset++;

      OIntegerSerializer.INSTANCE.serializeNative(data.length, content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      System.arraycopy(data, 0, content, offset, data.length);
      offset += data.length;

      return offset;
    }

    @Override
    public int fromStream(byte[] content, int offset) {
      updateMasterRecord = content[offset] > 0;
      offset++;

      int size = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      data = new byte[size];
      System.arraycopy(content, offset, data, 0, data.length);
      offset += size;

      return offset;
    }

    @Override
    public int serializedSize() {
      return OIntegerSerializer.INT_SIZE + data.length + 1;
    }

    @Override
    public boolean isUpdateMasterRecord() {
      return updateMasterRecord;
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

      if (updateMasterRecord != that.updateMasterRecord)
        return false;
      if (!Arrays.equals(data, that.data))
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = Arrays.hashCode(data);
      result = 31 * result + (updateMasterRecord ? 1 : 0);
      return result;
    }

    @Override
    public String toString() {
      return "TestRecord {size: "
          + (data.length + OIntegerSerializer.INT_SIZE + 1 + (OIntegerSerializer.INT_SIZE + 3) + ", updateMasterRecord : "
              + updateMasterRecord + "}");
    }
  }

}
