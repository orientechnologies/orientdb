package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.collection.OLRUCache;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OPerformanceStatisticManager;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

import static org.mockito.Matchers.longThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Andrey Lomakin
 * @since 29.04.13
 */
@Test
public class WriteAheadLogTest {
  private static final int ONE_KB       = 1024;
  public static final  int SEGMENT_SIZE = OWALPage.PAGE_SIZE * 4;
  private ODiskWriteAheadLog writeAheadLog;
  private File               testDir;

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

  private ODiskWriteAheadLog createWAL() throws IOException {
    return createWAL(2, SEGMENT_SIZE);
  }

  private ODiskWriteAheadLog createWAL(int maxPagesCacheSize, int maxSegmentSize) throws IOException {
    OLocalPaginatedStorage paginatedStorage = mock(OLocalPaginatedStorage.class);
    when(paginatedStorage.getName()).thenReturn("WriteAheadLogTest");
    when(paginatedStorage.getStoragePath()).thenReturn(testDir.getAbsolutePath());
    OStorageConfiguration configurationMock = mock(OStorageConfiguration.class);
    when(configurationMock.getLocaleInstance()).thenReturn(Locale.getDefault());
    when(paginatedStorage.getConfiguration()).thenReturn(configurationMock);
    when(paginatedStorage.getPerformanceStatisticManager())
        .thenReturn(new OPerformanceStatisticManager(paginatedStorage, Long.MAX_VALUE, -1));

    return new ODiskWriteAheadLog(maxPagesCacheSize, -1, maxSegmentSize, null, true, paginatedStorage, 10);
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
    OLogSequenceNumber checkPointOneLSN = writeAheadLog.logFuzzyCheckPointStart(new OLogSequenceNumber(-1, -1));
    writeAheadLog.logFullCheckpointEnd();

    OLogSequenceNumber checkPointTwoLSN = writeAheadLog.logFullCheckpointStart();
    writeAheadLog.logFullCheckpointEnd();

    OLogSequenceNumber checkPointThreeLSN = writeAheadLog.logFuzzyCheckPointStart(new OLogSequenceNumber(-1, -1));
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

    TestRecord writtenRecord = new TestRecord(-1, SEGMENT_SIZE, 58, false);
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
    TestRecord writtenRecord = new TestRecord(-1, SEGMENT_SIZE, 58, false);

    writeAheadLog.log(writtenRecord);
    OLogSequenceNumber masterLSN = writeAheadLog.logFuzzyCheckPointStart(new OLogSequenceNumber(-1, -1));

    OLogSequenceNumber end = writeAheadLog.logFuzzyCheckPointEnd();

    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), masterLSN);
    Assert.assertEquals(writeAheadLog.end(), end);

    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), masterLSN);
    Assert.assertEquals(writeAheadLog.end(), end);
  }

  public void testSecondMasterRecordUpdate() throws Exception {
    writeAheadLog.log(new TestRecord(-1, SEGMENT_SIZE, 58, false));

    writeAheadLog.logFuzzyCheckPointStart(new OLogSequenceNumber(-1, -1));
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.log(new TestRecord(-1, SEGMENT_SIZE, 58, false));

    OLogSequenceNumber checkpointLSN = writeAheadLog.logFuzzyCheckPointStart(new OLogSequenceNumber(-1, -1));
    OLogSequenceNumber end = writeAheadLog.logFuzzyCheckPointEnd();

    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), checkpointLSN);
    Assert.assertEquals(writeAheadLog.end(), end);
    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), checkpointLSN);
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), end);
  }

  public void testThirdMasterRecordUpdate() throws Exception {
    writeAheadLog.log(new TestRecord(-1, SEGMENT_SIZE, 58, false));

    writeAheadLog.logFuzzyCheckPointStart(new OLogSequenceNumber(-1, -1));
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.log(new TestRecord(-1, SEGMENT_SIZE, 58, false));

    writeAheadLog.logFuzzyCheckPointStart(new OLogSequenceNumber(-1, -1));
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.log(new TestRecord(-1, SEGMENT_SIZE, 58, false));

    OLogSequenceNumber checkpointLSN = writeAheadLog.logFuzzyCheckPointStart(new OLogSequenceNumber(-1, -1));
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

    int recordDistance;
    // first page
    recordDistance = ONE_KB;
    OLogSequenceNumber lsn;
    long nextStart = 0;
    long lastPosition = 0;

    TestRecord walRecord = new TestRecord(-1, SEGMENT_SIZE, recordDistance, false);
    lsn = writeAheadLog.log(walRecord);
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);
    nextStart += recordDistance;

    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    recordDistance = ONE_KB;
    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, recordDistance, false);
    lsn = writeAheadLog.log(walRecord);
    lastPosition = walRecord.recordEnd;
    nextStart += recordDistance;

    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    writtenRecords.add(walRecord);

    recordDistance = OWALPage.PAGE_SIZE - 2 * ONE_KB;
    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, recordDistance, false);
    lsn = writeAheadLog.log(walRecord);
    lastPosition = walRecord.recordEnd;
    nextStart += recordDistance;

    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    Assert.assertEquals(writeAheadLog.size(), OWALPage.PAGE_SIZE);
    writtenRecords.add(walRecord);

    // second page
    recordDistance = ONE_KB;
    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, recordDistance, false);
    lsn = writeAheadLog.log(walRecord);
    lastPosition = walRecord.recordEnd;
    nextStart += ONE_KB;

    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    writtenRecords.add(walRecord);

    recordDistance = ONE_KB;
    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, recordDistance, false);
    lsn = writeAheadLog.log(walRecord);
    lastPosition = walRecord.recordEnd;
    nextStart += ONE_KB;

    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    writtenRecords.add(walRecord);

    recordDistance = OWALPage.PAGE_SIZE - 2 * ONE_KB;
    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, recordDistance, false);
    lsn = writeAheadLog.log(walRecord);
    nextStart += recordDistance;
    lastPosition = walRecord.recordEnd;

    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    Assert.assertEquals(writeAheadLog.size(), 2 * OWALPage.PAGE_SIZE);

    writtenRecords.add(walRecord);

    // third page
    recordDistance = ONE_KB;
    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, recordDistance, false);
    lsn = writeAheadLog.log(walRecord);
    nextStart += recordDistance;
    lastPosition = walRecord.recordEnd;

    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    writtenRecords.add(walRecord);

    recordDistance = ONE_KB;
    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, recordDistance, false);
    lsn = writeAheadLog.log(walRecord);
    nextStart += recordDistance;
    lastPosition = walRecord.recordEnd;

    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    writtenRecords.add(walRecord);

    recordDistance = OWALPage.PAGE_SIZE - 2 * ONE_KB;
    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, recordDistance, false);
    lsn = writeAheadLog.log(walRecord);
    nextStart += recordDistance;
    lastPosition = walRecord.recordEnd;

    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    Assert.assertEquals(writeAheadLog.size(), 3 * OWALPage.PAGE_SIZE);
    writtenRecords.add(walRecord);

    // fourth page
    recordDistance = ONE_KB;
    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, recordDistance, false);
    lsn = writeAheadLog.log(walRecord);
    nextStart += recordDistance;
    lastPosition = walRecord.recordEnd;

    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    writtenRecords.add(walRecord);

    recordDistance = ONE_KB;
    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, recordDistance, false);
    lsn = writeAheadLog.log(walRecord);
    nextStart += recordDistance;
    lastPosition = walRecord.recordEnd;

    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    writtenRecords.add(walRecord);

    recordDistance = OWALPage.PAGE_SIZE - 2 * ONE_KB;
    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, recordDistance, false);
    lsn = writeAheadLog.log(walRecord);
    nextStart += recordDistance;
    lastPosition = walRecord.recordEnd;

    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    writtenRecords.add(walRecord);

    // fifth page
    recordDistance = ONE_KB;
    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, recordDistance, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    nextStart += recordDistance;

    writtenRecords.add(walRecord);

    assertLogContent(writeAheadLog, writtenRecords);

    Assert.assertEquals(writeAheadLog.end(), end);
    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.size(), nextStart);
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), end);

    assertLogContent(writeAheadLog, writtenRecords);
  }

  public void testWriteMultipleRecordsWithDifferentSizeAfterCloseOne() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    long nextStart;
    long lastPosition;

    // first page
    TestRecord walRecord = new TestRecord(-1, SEGMENT_SIZE, ONE_KB, false);
    writeAheadLog.log(walRecord);
    nextStart = ONE_KB;
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, ONE_KB, false);
    OLogSequenceNumber lsn = writeAheadLog.log(walRecord);
    lastPosition = walRecord.recordEnd;
    nextStart += ONE_KB;
    writtenRecords.add(walRecord);

    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    long seed = System.currentTimeMillis();
    System.out.println("seed of testWriteMultipleRecordsWithDifferentSizeAfterCloseOne " + seed);
    Random random = new Random(seed);
    writeAheadLog = createWAL();

    OLogSequenceNumber end = lsn;

    for (int writtenSize = 0; writtenSize < 4 * OWALPage.PAGE_SIZE; ) {
      int recordDistance = random.nextInt(2 * OWALPage.PAGE_SIZE - 1) + 15;
      walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, recordDistance, false);

      end = writeAheadLog.log(walRecord);
      nextStart += recordDistance;
      lastPosition = walRecord.recordEnd;
      writtenRecords.add(walRecord);

      writtenSize += recordDistance;

      assertLogContent(writeAheadLog, writtenRecords);
    }

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    assertLogContent(writeAheadLog, writtenRecords);

    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), end);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    assertLogContent(writeAheadLog, writtenRecords);
  }

  public void testWriteMultipleRecordsWithDifferentSizeAfterCloseTwo() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    long nextStart;
    long lastPosition;

    // first page
    TestRecord walRecord = new TestRecord(-1, SEGMENT_SIZE, ONE_KB, false);
    writeAheadLog.log(walRecord);
    nextStart = ONE_KB;
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, ONE_KB, false);
    writeAheadLog.log(walRecord);
    nextStart += ONE_KB;
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE - 2 * ONE_KB, false);
    OLogSequenceNumber lsn = writeAheadLog.log(walRecord);
    nextStart += OWALPage.PAGE_SIZE - 2 * ONE_KB;
    lastPosition = walRecord.recordEnd;

    Assert.assertEquals(writeAheadLog.size(), nextStart);

    writtenRecords.add(walRecord);

    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    long seed = System.currentTimeMillis();
    System.out.println("seed of testWriteMultipleRecordsWithDifferentSizeAfterCloseTwo " + seed);
    Random random = new Random(seed);
    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.size(), nextStart);

    OLogSequenceNumber end = lsn;
    for (int writtenSize = 0; writtenSize < 4 * OWALPage.PAGE_SIZE; ) {
      int recordDistance = random.nextInt(2 * OWALPage.PAGE_SIZE - 1) + 15;
      walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, recordDistance, false);

      end = writeAheadLog.log(walRecord);
      nextStart += recordDistance;
      lastPosition = walRecord.recordEnd;
      writtenRecords.add(walRecord);

      Assert.assertEquals(writeAheadLog.size(), nextStart);
      writtenSize += recordDistance;
    }

    assertLogContent(writeAheadLog, writtenRecords);

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.size(), nextStart);
    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), end);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    assertLogContent(writeAheadLog, writtenRecords);
  }

  public void testWriteMultipleRecordsWithDifferentSizeAfterCloseThree() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    long nextStart;
    long lastPosition;

    // first page
    TestRecord walRecord = new TestRecord(-1, SEGMENT_SIZE, ONE_KB, false);
    writeAheadLog.log(walRecord);
    nextStart = ONE_KB;
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, ONE_KB, false);
    writeAheadLog.log(walRecord);
    nextStart += ONE_KB;
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE - 2 * ONE_KB, false);
    OLogSequenceNumber lsn = writeAheadLog.log(walRecord);
    nextStart += OWALPage.PAGE_SIZE - 2 * ONE_KB;
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    long seed = System.currentTimeMillis();
    System.out.println("seed of testWriteMultipleRecordsWithDifferentSizeAfterCloseThree " + seed);
    Random random = new Random(seed);
    writeAheadLog = createWAL();

    OLogSequenceNumber end = lsn;

    for (int writtenSize = 0; writtenSize < 4 * OWALPage.PAGE_SIZE; ) {
      int recordDistance = random.nextInt(2 * OWALPage.PAGE_SIZE - 1) + 15;
      walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, recordDistance, false);

      end = writeAheadLog.log(walRecord);
      nextStart += recordDistance;
      lastPosition = walRecord.recordEnd;
      writtenRecords.add(walRecord);

      writtenSize += recordDistance;
    }

    assertLogContent(writeAheadLog, writtenRecords);

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), end);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    assertLogContent(writeAheadLog, writtenRecords);
  }

  public void testWriteMultipleRecordsWithDifferentSizeAfterCloseFour() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    long nextStart;
    long lastPosition;

    // first page
    TestRecord walRecord = new TestRecord(-1, SEGMENT_SIZE, ONE_KB, false);
    writeAheadLog.log(walRecord);
    nextStart = ONE_KB;
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, ONE_KB, false);
    writeAheadLog.log(walRecord);
    nextStart += ONE_KB;
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE - 2 * ONE_KB, false);
    OLogSequenceNumber lsn = writeAheadLog.log(walRecord);
    lastPosition = walRecord.recordEnd;
    nextStart += OWALPage.PAGE_SIZE - 2 * ONE_KB;

    writtenRecords.add(walRecord);

    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    long seed = System.currentTimeMillis();
    System.out.println("seed of testWriteMultipleRecordsWithDifferentSizeAfterCloseFour " + seed);
    Random random = new Random(seed);
    writeAheadLog = createWAL();

    OLogSequenceNumber end = lsn;
    for (int writtenSize = 0; writtenSize < 4 * OWALPage.PAGE_SIZE; ) {
      int recordDistance = random.nextInt(2 * OWALPage.PAGE_SIZE - 1) + 65;
      walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, recordDistance, false);

      end = writeAheadLog.log(walRecord);
      nextStart += recordDistance;
      lastPosition = walRecord.recordEnd;
      writtenRecords.add(walRecord);

      writtenSize += recordDistance;
    }

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.size(), nextStart);
    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), end);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    assertLogContent(writeAheadLog, writtenRecords);
  }

  public void testWriteMultipleRandomRecords() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    long seed = System.currentTimeMillis();
    System.out.println("seed of testWriteMultipleRecordsWithDifferentSizeAfterCloseFour " + seed);
    Random random = new Random(seed);

    OLogSequenceNumber lsn = null;
    long nextStart = 0;
    long lastPosition = 0;

    for (int writtenSize = 0; writtenSize < 16 * OWALPage.PAGE_SIZE; ) {
      int recordDistance = random.nextInt(2 * OWALPage.PAGE_SIZE - 1) + OWALPage.RECORDS_OFFSET + 10;

      TestRecord walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, recordDistance, false);

      lsn = writeAheadLog.log(walRecord);
      lastPosition = walRecord.recordEnd;

      nextStart += recordDistance;
      Assert.assertEquals(writeAheadLog.size(), nextStart);
      writtenRecords.add(walRecord);

      writtenSize += recordDistance;
    }

    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    writeAheadLog = createWAL();

    assertLogContent(writeAheadLog, writtenRecords);

    OLogSequenceNumber end = lsn;
    for (int writtenSize = 0; writtenSize < 16 * OWALPage.PAGE_SIZE; ) {
      int recordDistance = random.nextInt(2 * OWALPage.PAGE_SIZE - 1) + 2 * OIntegerSerializer.INT_SIZE + 5;

      TestRecord walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, recordDistance, false);

      end = writeAheadLog.log(walRecord);
      lastPosition = walRecord.recordEnd;

      nextStart += recordDistance;
      Assert.assertEquals(writeAheadLog.size(), nextStart);
      writtenRecords.add(walRecord);

      writtenSize += recordDistance;
    }

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), end);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    assertLogContent(writeAheadLog, writtenRecords);
  }

  public void testFlushedLSNOnePage() throws Exception {
    long nextStart;
    long lastPosition = 0;

    TestRecord walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, ONE_KB, false);
    writeAheadLog.log(walRecord);
    nextStart = ONE_KB;
    lastPosition = walRecord.recordEnd;

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, ONE_KB, false);
    writeAheadLog.log(walRecord);
    nextStart += ONE_KB;
    lastPosition = walRecord.recordEnd;

    Assert.assertNull(writeAheadLog.getFlushedLsn());

    writeAheadLog.flush();

    Assert.assertEquals(writeAheadLog.getFlushedLsn(), walRecord.getLsn());
    Assert.assertEquals(writeAheadLog.end(), walRecord.getLsn());
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    final OLogSequenceNumber end = walRecord.getLsn();

    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), end);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, ONE_KB, false);
    OLogSequenceNumber lsn = writeAheadLog.log(walRecord);
    nextStart += ONE_KB;

    Assert.assertEquals(writeAheadLog.getFlushedLsn(), end);
    Assert.assertEquals(end.compareTo(lsn), -1);
    Assert.assertEquals(writeAheadLog.end(), walRecord.getLsn());
    Assert.assertEquals(writeAheadLog.size(), nextStart);
  }

  public void testFlushedLSNOnePageWithLessThanMinRecordSpace() throws Exception {
    long nextStart;
    long lastPosition;

    TestRecord walRecord = new TestRecord(-1, SEGMENT_SIZE, ONE_KB, false);
    writeAheadLog.log(walRecord);
    nextStart = ONE_KB;
    lastPosition = walRecord.recordEnd;

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, ONE_KB, false);
    writeAheadLog.log(walRecord);
    nextStart += ONE_KB;
    lastPosition = walRecord.recordEnd;

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE - 2 * ONE_KB, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    nextStart += OWALPage.PAGE_SIZE - 2 * ONE_KB;
    lastPosition = walRecord.recordEnd;

    Assert.assertNull(writeAheadLog.getFlushedLsn());

    writeAheadLog.flush();

    Assert.assertEquals(writeAheadLog.getFlushedLsn(), end);
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), walRecord.getLsn());
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    final OLogSequenceNumber flushedLSN = walRecord.getLsn();

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, ONE_KB, false);
    OLogSequenceNumber lsn = writeAheadLog.log(walRecord);
    nextStart += ONE_KB;

    Assert.assertEquals(writeAheadLog.getFlushedLsn(), flushedLSN);

    Assert.assertEquals(end.compareTo(lsn), -1);
    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), nextStart);
  }

  public void testFlushedLSNOnePageWithMinRecordSpace() throws Exception {
    long nextStart;
    long lastPosition;

    TestRecord walRecord = new TestRecord(-1, SEGMENT_SIZE, ONE_KB, false);
    writeAheadLog.log(walRecord);
    nextStart = ONE_KB;
    lastPosition = walRecord.recordEnd;

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, ONE_KB, false);
    writeAheadLog.log(walRecord);
    nextStart += ONE_KB;
    lastPosition = walRecord.recordEnd;

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE - 2 * ONE_KB, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    nextStart += OWALPage.PAGE_SIZE - 2 * ONE_KB;
    lastPosition = walRecord.recordEnd;

    Assert.assertNull(writeAheadLog.getFlushedLsn());

    writeAheadLog.flush();

    Assert.assertEquals(writeAheadLog.getFlushedLsn(), walRecord.getLsn());
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), walRecord.getLsn());
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    end = writeAheadLog.end();

    OLogSequenceNumber flushedLSN = walRecord.getLsn();

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, ONE_KB, false);
    OLogSequenceNumber lsn = writeAheadLog.log(walRecord);
    nextStart += ONE_KB;

    Assert.assertEquals(writeAheadLog.getFlushedLsn(), flushedLSN);
    Assert.assertEquals(end.compareTo(lsn), -1);
    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), nextStart);
  }

  public void testFlushedLSNOnePageWithNoSpace() throws Exception {
    OLogSequenceNumber lsn;
    long nextStart;
    long lastPosition = 0;

    TestRecord walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, ONE_KB, false);
    lsn = writeAheadLog.log(walRecord);
    nextStart = ONE_KB;
    lastPosition = walRecord.recordEnd;

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, ONE_KB, false);
    lsn = writeAheadLog.log(walRecord);
    nextStart += ONE_KB;
    lastPosition = walRecord.recordEnd;

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE - 2 * ONE_KB, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    nextStart += OWALPage.PAGE_SIZE - 2 * ONE_KB;
    lastPosition = walRecord.recordEnd;

    Assert.assertNull(writeAheadLog.getFlushedLsn());

    writeAheadLog.flush();

    Assert.assertEquals(writeAheadLog.getFlushedLsn(), walRecord.getLsn());
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), walRecord.getLsn());
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    OLogSequenceNumber flushedLSN = walRecord.getLsn();
    end = writeAheadLog.end();

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, ONE_KB, false);
    lsn = writeAheadLog.log(walRecord);
    nextStart += ONE_KB;

    Assert.assertEquals(writeAheadLog.getFlushedLsn(), flushedLSN);
    Assert.assertEquals(end.compareTo(lsn), -1);
    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), nextStart);
  }

  public void testFlushedLSNTwoPagesAndThenTwo() throws Exception {
    long nextStart = 0;
    long lastPosition = -1;

    TestRecord walRecord = null;
    for (int i = 0; i < 2; i++) {
      walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, ONE_KB, false);
      writeAheadLog.log(walRecord);
      nextStart += ONE_KB;
      lastPosition = walRecord.recordEnd;

      walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, ONE_KB, false);
      writeAheadLog.log(walRecord);
      nextStart += ONE_KB;
      lastPosition = walRecord.recordEnd;

      walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE - 2 * ONE_KB, false);
      writeAheadLog.log(walRecord);
      nextStart += OWALPage.PAGE_SIZE - 2 * ONE_KB;
      lastPosition = walRecord.recordEnd;
    }

    Assert.assertNull(writeAheadLog.getFlushedLsn());

    writeAheadLog.flush();

    OLogSequenceNumber end = null;
    for (int i = 0; i < 2; i++) {
      walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, ONE_KB, false);
      writeAheadLog.log(walRecord);
      nextStart += ONE_KB;
      lastPosition = walRecord.recordEnd;

      walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, ONE_KB, false);
      writeAheadLog.log(walRecord);
      nextStart += ONE_KB;
      lastPosition = walRecord.recordEnd;

      walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE - 2 * ONE_KB, false);
      end = writeAheadLog.log(walRecord);
      nextStart += OWALPage.PAGE_SIZE - 2 * ONE_KB;
      lastPosition = walRecord.recordEnd;
    }

    writeAheadLog.flush();
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), walRecord.getLsn());
    Assert.assertEquals(writeAheadLog.end(), end);
    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), walRecord.getLsn());

    OLogSequenceNumber flushedLSN = walRecord.getLsn();

    end = writeAheadLog.end();

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, ONE_KB, false);
    OLogSequenceNumber lsn = writeAheadLog.log(walRecord);
    nextStart += ONE_KB;

    Assert.assertEquals(writeAheadLog.getFlushedLsn(), flushedLSN);

    Assert.assertEquals(end.compareTo(lsn), -1);
    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), nextStart);
  }

  public void testFlushedLSNTwoPagesOneWithTrail() throws Exception {
    long nextStart;
    long lastPosition = 0;

    TestRecord walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, ONE_KB, false);
    writeAheadLog.log(walRecord);
    nextStart = ONE_KB;
    lastPosition = walRecord.recordEnd;

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, ONE_KB, false);
    writeAheadLog.log(walRecord);
    nextStart += ONE_KB;
    lastPosition = walRecord.recordEnd;

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE - 2 * ONE_KB, false);
    writeAheadLog.log(walRecord);
    nextStart += OWALPage.PAGE_SIZE - 2 * ONE_KB;
    lastPosition = walRecord.recordEnd;

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, ONE_KB, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    nextStart += ONE_KB;
    lastPosition = walRecord.recordEnd;

    Assert.assertNull(writeAheadLog.getFlushedLsn());

    writeAheadLog.flush();
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), walRecord.getLsn());
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), walRecord.getLsn());
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    OLogSequenceNumber flushedLSN = walRecord.getLsn();
    end = writeAheadLog.end();

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, ONE_KB, false);
    OLogSequenceNumber lsn = writeAheadLog.log(walRecord);
    nextStart += ONE_KB;

    Assert.assertEquals(writeAheadLog.getFlushedLsn(), flushedLSN);
    Assert.assertEquals(end.compareTo(lsn), -1);
    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), nextStart);
  }

  public void testFlushedLSNTwoTwoSegments() throws Exception {
    long seek = System.currentTimeMillis();
    System.out.println("testFlushedLSNTwoTwoSegments seek " + seek);
    Random random = new Random(seek);

    int writtenContent = 0;
    TestRecord walRecord;
    long nextStart = 0;
    long lastPosition = 0;

    while (writtenContent <= 4 * OWALPage.PAGE_SIZE) {
      int recordDuration = random.nextInt(OWALPage.PAGE_SIZE - 1) + 15;

      walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, recordDuration, false);
      writeAheadLog.log(walRecord);
      nextStart += recordDuration;
      lastPosition = walRecord.recordEnd;

      writtenContent += recordDuration;
    }

    int recordDuration = random.nextInt(OWALPage.PAGE_SIZE - 1) + 15;
    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, recordDuration, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    nextStart += recordDuration;
    lastPosition = walRecord.recordEnd;

    writeAheadLog.flush();

    Assert.assertEquals(writeAheadLog.getFlushedLsn(), walRecord.getLsn());
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), walRecord.getLsn());
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    end = writeAheadLog.end();
    OLogSequenceNumber flushedLSN = walRecord.getLsn();

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, ONE_KB, false);
    OLogSequenceNumber lsn = writeAheadLog.log(walRecord);
    nextStart += ONE_KB;

    Assert.assertEquals(writeAheadLog.getFlushedLsn(), flushedLSN);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    Assert.assertEquals(end.compareTo(lsn), -1);
    Assert.assertEquals(writeAheadLog.end(), lsn);
  }

  public void testFirstMasterRecordIsBrokenSingleRecord() throws Exception {
    writeAheadLog.log(new TestRecord(-1, SEGMENT_SIZE, 58, false));

    writeAheadLog.logFuzzyCheckPointStart(new OLogSequenceNumber(-1, -1));
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
    writeAheadLog.log(new TestRecord(-1, SEGMENT_SIZE, 58, false));

    OLogSequenceNumber checkPointLSN = writeAheadLog.logFuzzyCheckPointStart(new OLogSequenceNumber(-1, -1));
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.log(new TestRecord(-1, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false));

    writeAheadLog.logFuzzyCheckPointStart(new OLogSequenceNumber(-1, -1));
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
    writeAheadLog.log(new TestRecord(-1, SEGMENT_SIZE, 58, false));

    writeAheadLog.logFuzzyCheckPointStart(new OLogSequenceNumber(-1, -1));
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.log(new TestRecord(-1, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false));

    OLogSequenceNumber checkPointLSN = writeAheadLog.logFuzzyCheckPointStart(new OLogSequenceNumber(-1, -1));
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.log(new TestRecord(-1, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false));

    writeAheadLog.logFuzzyCheckPointStart(new OLogSequenceNumber(-1, -1));
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
    long lastPosition = 0;

    for (int i = 0; i < recordsToWrite; i++) {
      TestRecord setPageDataRecord = new TestRecord(lastPosition, SEGMENT_SIZE, 65, false);
      writtenRecords.add(setPageDataRecord);

      end = writeAheadLog.log(setPageDataRecord);
      lastPosition = setPageDataRecord.recordEnd;
    }

    assertLogContent(writeAheadLog, writtenRecords);
    assertLogContent(writeAheadLog, writtenRecords.subList(writtenRecords.size() / 2, writtenRecords.size()));

    Assert.assertEquals(writeAheadLog.end(), end);
    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), end);

    assertLogContent(writeAheadLog, writtenRecords);
    assertLogContent(writeAheadLog, writtenRecords.subList(writtenRecords.size() / 2, writtenRecords.size()));
  }

  public void testAppendMultipleRecordsAfterClose() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    OLogSequenceNumber end = null;
    long nextStart = 0;
    long lastPosition = 0;

    final int recordsToWrite = 1;
    for (int i = 0; i < recordsToWrite; i++) {
      TestRecord testRecord = new TestRecord(lastPosition, SEGMENT_SIZE, 65, false);
      writtenRecords.add(testRecord);

      end = writeAheadLog.log(testRecord);
      nextStart += 65;
      lastPosition = testRecord.recordEnd;
    }

    Assert.assertEquals(writeAheadLog.end(), end);
    writeAheadLog.close();
    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), end);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    for (int i = 0; i < recordsToWrite; i++) {
      TestRecord testRecord = new TestRecord(lastPosition, SEGMENT_SIZE, 65, false);
      writtenRecords.add(testRecord);

      end = writeAheadLog.log(testRecord);
      nextStart += 65;
      lastPosition = testRecord.recordEnd;
    }

    assertLogContent(writeAheadLog, writtenRecords);
    assertLogContent(writeAheadLog, writtenRecords.subList(writtenRecords.size() / 2, writtenRecords.size()));

    Assert.assertEquals(writeAheadLog.end(), end);
    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), end);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    assertLogContent(writeAheadLog, writtenRecords);
    assertLogContent(writeAheadLog, writtenRecords.subList(writtenRecords.size() / 2, writtenRecords.size()));
  }

  public void testPageIsBrokenOnOtherSegment() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    long lastPosition = 0;

    TestRecord walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, ONE_KB, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    lastPosition = walRecord.recordEnd;

    writtenRecords.add(walRecord);

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, ONE_KB, false);
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

    Assert.assertEquals(writeAheadLog.size(), ONE_KB);
    Assert.assertEquals(writeAheadLog.end(), end);
    assertLogContent(writeAheadLog, writtenRecords.subList(0, 1));

    final OWALRecord brokenRecord = writeAheadLog.read(writtenRecords.get(1).getLsn());
    Assert.assertNull(brokenRecord);
  }

  public void testPageIsBrokenThreeSegmentsOneRecordIsTwoPageWide() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    long lastPosition = 0;

    TestRecord walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, ONE_KB, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, 2 * OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, ONE_KB, false);
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
    Assert.assertEquals(writeAheadLog.size(), ONE_KB);
    assertLogContent(writeAheadLog, writtenRecords.subList(0, 1));

    final OWALRecord brokenRecord = writeAheadLog.read(writtenRecords.get(1).getLsn());
    Assert.assertNull(brokenRecord);
  }

  public void testPageIsBrokenAndEmpty() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    long lastPosition = 0;

    TestRecord walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, 3 * OWALPage.PAGE_SIZE, false);
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

    Assert.assertEquals(writeAheadLog.end(), writtenRecords.get(0).getLsn());
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), writtenRecords.get(0).getLsn());
    Assert.assertEquals(writeAheadLog.size(), OWALPage.PAGE_SIZE);
    assertLogContent(writeAheadLog, writtenRecords.subList(0, 1));

    final OWALRecord brokenRecord = writeAheadLog.read(writtenRecords.get(1).getLsn());
    Assert.assertNull(brokenRecord);
  }

  public void testSecondPageWasTruncated() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    long latestPosition = 0;

    TestRecord walRecord = new TestRecord(latestPosition, SEGMENT_SIZE, 100, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    latestPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(latestPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    writeAheadLog.close();

    RandomAccessFile rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "rw");
    rndFile.setLength(OWALPage.PAGE_SIZE);
    rndFile.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), end);
    Assert.assertEquals(writeAheadLog.size(), 100);

    assertLogContent(writeAheadLog, writtenRecords.subList(0, 1));

    final OWALRecord brokenRecord = writeAheadLog.read(writtenRecords.get(1).getLsn());
    Assert.assertNull(brokenRecord);
  }

  public void testThirdPageWasTruncated() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    long latestPosition = 0;

    TestRecord walRecord = new TestRecord(latestPosition, SEGMENT_SIZE, 100, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    latestPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(latestPosition, SEGMENT_SIZE, 2 * OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    writeAheadLog.close();

    RandomAccessFile rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "rw");
    rndFile.setLength(2 * OWALPage.PAGE_SIZE);
    rndFile.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), end);
    Assert.assertEquals(writeAheadLog.size(), 100);

    assertLogContent(writeAheadLog, writtenRecords.subList(0, 1));

    final OWALRecord brokenRecord = writeAheadLog.read(writtenRecords.get(1).getLsn());
    Assert.assertNull(brokenRecord);
  }

  public void testThirdPageCRCWasIncorrect() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    long latestPosition = 0;

    TestRecord walRecord = new TestRecord(latestPosition, SEGMENT_SIZE, 100, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    latestPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(latestPosition, SEGMENT_SIZE, 2 * OWALPage.PAGE_SIZE, false);
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
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), end);
    Assert.assertEquals(writeAheadLog.size(), 100);

    assertLogContent(writeAheadLog, writtenRecords.subList(0, 1));

    final OWALRecord brokenRecord = writeAheadLog.read(writtenRecords.get(1).getLsn());
    Assert.assertNull(brokenRecord);
  }

  public void testFirstPageInFlushWasBroken() throws Exception {
    long latestPosition = 0;

    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    // first flush
    TestRecord walRecord = new TestRecord(latestPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    latestPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(latestPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    latestPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    // second flush
    walRecord = new TestRecord(latestPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    latestPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(latestPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
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
    Assert.assertEquals(writeAheadLog.size(), 4 * OWALPage.PAGE_SIZE);
    assertLogContent(writeAheadLog, writtenRecords.subList(0, 2));
    try {
      writeAheadLog.read(writtenRecords.get(2).getLsn());
      Assert.fail();
    } catch (OWALPageBrokenException e) {
    }
  }

  public void testFirstInCompletePageInFlushWasBroken() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();
    OLogSequenceNumber lsn;
    long latestPosition = 0;

    // first flush
    TestRecord walRecord = new TestRecord(latestPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false);
    lsn = writeAheadLog.log(walRecord);
    latestPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(latestPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE + 100, false);
    lsn = writeAheadLog.log(walRecord);
    latestPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    // second flush
    walRecord = new TestRecord(latestPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE - 100, false);
    lsn = writeAheadLog.log(walRecord);
    latestPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(latestPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false);

    OLogSequenceNumber end = writeAheadLog.log(walRecord);
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
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), end);

    Assert.assertEquals(writeAheadLog.size(), 4 * OWALPage.PAGE_SIZE);

    assertLogContent(writeAheadLog, writtenRecords.subList(0, 1));
    try {
      writeAheadLog.read(writtenRecords.get(1).getLsn());
      Assert.fail();
    } catch (OWALPageBrokenException e) {
    }
  }

  public void testMiddlePageInFlushWasBroken() throws Exception {
    long nextStart = 0;
    long lastPosition = 0;

    writeAheadLog.close();
    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    // first flush
    TestRecord walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    nextStart += OWALPage.PAGE_SIZE;
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    nextStart += OWALPage.PAGE_SIZE;
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);
    writeAheadLog.flush();

    // second flush
    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    nextStart += OWALPage.PAGE_SIZE;
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    nextStart += OWALPage.PAGE_SIZE;
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    nextStart += OWALPage.PAGE_SIZE;
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
    Assert.assertEquals(writeAheadLog.size(), nextStart);
    try {
      writeAheadLog.read(writtenRecords.get(3).getLsn());
      Assert.fail();
    } catch (OWALPageBrokenException e) {
    }
  }

  public void testMiddleIncompletePageInFlushWasBroken() throws Exception {
    writeAheadLog.close();
    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    long nextStart = 0;
    long lastPosition = 0;

    // first flush
    TestRecord walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE + 100, false);
    writeAheadLog.log(walRecord);
    nextStart = OWALPage.PAGE_SIZE + 100;
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    nextStart += OWALPage.PAGE_SIZE;
    lastPosition = walRecord.recordEnd;
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    writtenRecords.add(walRecord);
    writeAheadLog.flush();

    // second flush
    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    nextStart += OWALPage.PAGE_SIZE;
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE - 100, false);
    OLogSequenceNumber lsn = writeAheadLog.log(walRecord);
    nextStart += OWALPage.PAGE_SIZE - 100;
    writtenRecords.add(walRecord);
    lastPosition = walRecord.recordEnd;
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    nextStart += OWALPage.PAGE_SIZE;
    writtenRecords.add(walRecord);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    writeAheadLog.close();

    RandomAccessFile rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "rw");
    rndFile.seek(3 * OWALPage.PAGE_SIZE);
    int bt = rndFile.read();
    rndFile.seek(3 * OWALPage.PAGE_SIZE);
    rndFile.write(bt + 1);
    rndFile.close();

    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    assertLogContent(writeAheadLog, writtenRecords.subList(0, 2));
    try {
      writeAheadLog.read(writtenRecords.get(2).getLsn());
      Assert.fail();
    } catch (OWALPageBrokenException e) {
    }
  }

  public void testFirstPageWasNotFlushedFirstCase() throws Exception {
    writeAheadLog.close();
    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    long nextStart = 0;
    long lastPosition = 0;

    // first flush
    TestRecord walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    nextStart = OWALPage.PAGE_SIZE;
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE + 100, false);
    writeAheadLog.log(walRecord);
    nextStart += OWALPage.PAGE_SIZE + 100;
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);
    writeAheadLog.flush();

    RandomAccessFile rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "r");
    byte[] content = new byte[OWALPage.PAGE_SIZE];
    rndFile.seek(2 * OWALPage.PAGE_SIZE);
    rndFile.readFully(content);
    rndFile.close();

    // second flush
    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE - 100, false);
    writeAheadLog.log(walRecord);
    nextStart += OWALPage.PAGE_SIZE - 100;
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false);
    OLogSequenceNumber lsn = writeAheadLog.log(walRecord);
    nextStart += OWALPage.PAGE_SIZE;
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    nextStart += OWALPage.PAGE_SIZE;
    writtenRecords.add(walRecord);

    writeAheadLog.close();

    rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "rw");
    rndFile.seek(2 * OWALPage.PAGE_SIZE);
    rndFile.write(content);
    rndFile.close();

    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.size(), nextStart);
    assertLogContent(writeAheadLog, writtenRecords.subList(0, 1));

    try {
      writeAheadLog.read(writtenRecords.get(1).getLsn());
      Assert.fail();
    } catch (OWALPageBrokenException e) {
    }
  }

  public void testFirstPageWasNotFlushedSecondCase() throws Exception {
    writeAheadLog.close();
    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    long lastPosition = 0;
    long nextStart = 0;

    // first flush
    TestRecord walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE - 100, false);
    writeAheadLog.log(walRecord);
    nextStart = OWALPage.PAGE_SIZE - 100;
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    writeAheadLog.flush();

    RandomAccessFile rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "r");
    byte[] content = new byte[OWALPage.PAGE_SIZE];
    rndFile.seek(0);
    rndFile.readFully(content);
    rndFile.close();

    // second flush
    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE + 100, false);
    writeAheadLog.log(walRecord);
    nextStart += OWALPage.PAGE_SIZE + 100;
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE - 100, false);
    writeAheadLog.log(walRecord);
    nextStart += OWALPage.PAGE_SIZE - 100;
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    nextStart += OWALPage.PAGE_SIZE;
    writtenRecords.add(walRecord);

    writeAheadLog.close();

    rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "rw");
    rndFile.seek(0);
    rndFile.write(content);
    rndFile.close();

    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.size(), nextStart);

    try {
      writeAheadLog.read(writtenRecords.get(0).getLsn());
      Assert.fail();
    } catch (OWALPageBrokenException e) {
    }
  }

  public void testPageWasNotFullyWritten() throws Exception {
    writeAheadLog.close();
    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();

    long nextStart = 0;
    long lastPosition = 0;

    // first flush
    TestRecord walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    nextStart += OWALPage.PAGE_SIZE;
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    nextStart += OWALPage.PAGE_SIZE;
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);
    writeAheadLog.flush();

    // second flush
    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    nextStart += OWALPage.PAGE_SIZE;
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    nextStart += OWALPage.PAGE_SIZE;
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false);
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
    Assert.assertEquals(writeAheadLog.size(), nextStart);
  }

  public void testIncompletePageWasNotFullyWritten() throws Exception {
    writeAheadLog.close();
    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    List<OWALRecord> writtenRecords = new ArrayList<OWALRecord>();
    long nextStart = 0;
    long lastPosition = 0;

    // first flush
    TestRecord walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    nextStart = OWALPage.PAGE_SIZE;
    writtenRecords.add(walRecord);
    lastPosition = walRecord.recordEnd;

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    nextStart += OWALPage.PAGE_SIZE;
    lastPosition = walRecord.recordEnd;

    writtenRecords.add(walRecord);
    writeAheadLog.flush();

    // second flush
    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    nextStart += OWALPage.PAGE_SIZE;
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE + 100, false);
    writeAheadLog.log(walRecord);
    lastPosition = walRecord.recordEnd;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(lastPosition, SEGMENT_SIZE, OWALPage.PAGE_SIZE - 200, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    writeAheadLog.close();

    RandomAccessFile rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "rw");
    rndFile.setLength(rndFile.length() - OWALPage.PAGE_SIZE / 2);
    rndFile.close();

    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), end);
    Assert.assertEquals(writeAheadLog.size(), nextStart);
    assertLogContent(writeAheadLog, writtenRecords.subList(0, 3));
  }

  public void testTruncateFirstSegment() throws IOException {
    writeAheadLog.close();
    writeAheadLog = createWAL(6, 3 * OWALPage.PAGE_SIZE);
    final long segmentSize = 3 * OWALPage.PAGE_SIZE;

    OLogSequenceNumber lsn;
    long lastPosition = 0;

    TestRecord walRecord = new TestRecord(lastPosition, segmentSize, OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);

    walRecord = new TestRecord(lastPosition, segmentSize, OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    lastPosition = walRecord.recordEnd;

    walRecord = new TestRecord(lastPosition, segmentSize, 2 * OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    lastPosition = walRecord.recordEnd;

    walRecord = new TestRecord(lastPosition, segmentSize, OWALPage.PAGE_SIZE / 2, false);
    lsn = writeAheadLog.log(walRecord);

    writeAheadLog.cutTill(lsn);

    final OLogSequenceNumber startLSN = writeAheadLog.begin();
    Assert.assertEquals(startLSN, lsn);
    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), OWALPage.PAGE_SIZE / 2);
  }

  public void testTruncateLastSegment() throws IOException {
    writeAheadLog.close();
    writeAheadLog = createWAL(6, 3 * OWALPage.PAGE_SIZE);

    final long segmentSize = 3 * OWALPage.PAGE_SIZE;

    long lastPosition = 0;

    TestRecord walRecord = new TestRecord(lastPosition, segmentSize, OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    lastPosition = walRecord.recordEnd;

    walRecord = new TestRecord(lastPosition, segmentSize, OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    lastPosition = walRecord.recordEnd;

    walRecord = new TestRecord(lastPosition, segmentSize, 2 * OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    lastPosition = walRecord.recordEnd;

    // second segment
    walRecord = new TestRecord(lastPosition, segmentSize, OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    lastPosition = walRecord.recordEnd;

    walRecord = new TestRecord(lastPosition, segmentSize, OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    lastPosition = walRecord.recordEnd;

    walRecord = new TestRecord(lastPosition, segmentSize, OWALPage.PAGE_SIZE, false);
    writeAheadLog.log(walRecord);
    lastPosition = walRecord.recordEnd;

    // last segment
    walRecord = new TestRecord(lastPosition, segmentSize, OWALPage.PAGE_SIZE / 2, false);
    OLogSequenceNumber lsn = writeAheadLog.log(walRecord);

    writeAheadLog.cutTill(lsn);

    final OLogSequenceNumber startLSN = writeAheadLog.begin();
    Assert.assertEquals(startLSN, lsn);
    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), OWALPage.PAGE_SIZE / 2);
  }

  private void assertLogContent(ODiskWriteAheadLog writeAheadLog, List<? extends OWALRecord> writtenRecords) throws Exception {
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

    OLogSequenceNumber nextLsn = writeAheadLog.next(readRecord.getLsn());
    if (nextLsn != null)
      try {
        writeAheadLog.read(nextLsn);
        Assert.fail();
      } catch (OWALPageBrokenException e) {
      }
  }

  public static final class TestRecord extends OAbstractWALRecord {
    private byte[]  data;
    private boolean updateMasterRecord;
    /**
     * End position of current record in WAL segment.
     */
    private long    recordEnd;

    public TestRecord() {
    }

    /**
     * @param recordPositionEnd  Position in the file the end of the last record written in WAL segment, so position of the end of
     *                           the record which is written in the WAL just before this record
     * @param segmentSize        Maximum size of the single WAL segment
     * @param distance           Required distance in WAL file between the begging of current record and its end. In other word we
     *                           express how many space we can cover in WAL file by writing this record.
     * @param updateMasterRecord Flag which indicates whether LSN of this record should be stored in WAL master records registry
     */
    public TestRecord(long recordPositionEnd, long segmentSize, int distance, boolean updateMasterRecord) {
      Random random = new Random();

      //if end of the last record cross boundary new record will start
      //at the start of the next segment
      if (recordPositionEnd < 0 || recordPositionEnd / segmentSize > 0) {
        recordPositionEnd = 0;
      }

      this.recordEnd = recordPositionEnd + distance;

      //if we add record to a new page, some of the required space will be covered by
      //system information
      if (recordPositionEnd % OWALPage.PAGE_SIZE == 0) {
        recordPositionEnd += OWALPage.RECORDS_OFFSET;
        distance -= OWALPage.RECORDS_OFFSET;
      }

      if (distance <= 0) {
        throw new IllegalArgumentException("Data size for distance " + distance + " can not be calculated");
      }

      int finalSize;

      //free space in the page equals top position of end of last record minus page size
      int freeFirstPageSpace = OWALPage.PAGE_SIZE - (int) (recordPositionEnd % OWALPage.PAGE_SIZE);

      if (distance <= freeFirstPageSpace) {
        //take in account that despite user data some service data are added in each wal record
        finalSize = OWALPage.calculateRecordSize(distance);
      } else {
        distance -= freeFirstPageSpace;
        finalSize = OWALPage.calculateRecordSize(freeFirstPageSpace);

        final int amountOfFullPieces = distance / OWALPage.PAGE_SIZE;
        distance -= amountOfFullPieces * OWALPage.PAGE_SIZE;

        finalSize += amountOfFullPieces * OWALPage.calculateRecordSize(OWALPage.MAX_ENTRY_SIZE);

        if (distance > 0) {
          if (distance < OWALPage.RECORDS_OFFSET)
            throw new IllegalArgumentException("Data size for distance " + distance + " can not be calculated");

          distance -= OWALPage.RECORDS_OFFSET;
          finalSize += OWALPage.calculateRecordSize(distance);
        }
      }

      //we need to subtract serialization overhead (content length), boolean type, wal record type itself
      if (finalSize - OIntegerSerializer.INT_SIZE - 1 - 1 < 1) {
        throw new IllegalArgumentException("Can not create record with distance " + distance);
      }

      data = new byte[finalSize - OIntegerSerializer.INT_SIZE - 1 - 1];
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
      return "TestRecord {size: " + (data.length + OIntegerSerializer.INT_SIZE + 1 + (OIntegerSerializer.INT_SIZE + 3)
          + ", updateMasterRecord : " + updateMasterRecord + "}");
    }
  }

}
