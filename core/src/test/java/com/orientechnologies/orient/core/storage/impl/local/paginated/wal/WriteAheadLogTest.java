package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OStorageConfigurationImpl;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationMetadata;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OPerformanceStatisticManager;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Paths;
import java.util.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Andrey Lomakin
 * @since 29.04.13
 */
public class WriteAheadLogTest {
  private static final int ONE_KB       = 1024;
  private static final int SEGMENT_SIZE = OWALPage.PAGE_SIZE * 4;
  private        ODiskWriteAheadLog writeAheadLog;
  private static File               testDir;

  @BeforeClass
  public static void beforeClass() {
    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null || buildDirectory.isEmpty())
      buildDirectory = ".";

    testDir = new File(buildDirectory, "writeAheadLogTest");
    OFileUtils.deleteRecursively(testDir);
    Assert.assertTrue(testDir.mkdir());
    OWALRecordsFactory.INSTANCE.registerNewRecord((byte) 128, TestRecord.class);
  }

  @Before
  public void beforeMethod() throws Exception {
    writeAheadLog = createWAL();
  }

  private ODiskWriteAheadLog createWAL() throws IOException {
    return createWAL(2, SEGMENT_SIZE);
  }

  private ODiskWriteAheadLog createWAL(int maxPagesCacheSize, int maxSegmentSize) throws IOException {
    OLocalPaginatedStorage paginatedStorage = mock(OLocalPaginatedStorage.class);
    when(paginatedStorage.getName()).thenReturn("WriteAheadLogTest");
    when(paginatedStorage.getStoragePath()).thenReturn(Paths.get(testDir.getAbsolutePath()));
    OStorageConfigurationImpl configurationMock = mock(OStorageConfigurationImpl.class);
    when(configurationMock.getLocaleInstance()).thenReturn(Locale.getDefault());
    when(paginatedStorage.getConfiguration()).thenReturn(configurationMock);
    when(paginatedStorage.getPerformanceStatisticManager())
        .thenReturn(new OPerformanceStatisticManager(paginatedStorage, Long.MAX_VALUE, -1));

    return new ODiskWriteAheadLog(maxPagesCacheSize, -1, maxSegmentSize, null, true, paginatedStorage, 16 * OWALPage.PAGE_SIZE, 1);
  }

  @After
  public void afterMethod() throws Exception {
    if (writeAheadLog != null)
      writeAheadLog.delete();

    final File[] files = testDir.listFiles();
    if (files != null) {
      for (File file : files) {
        OFileUtils.deleteRecursively(file);
      }
    }
  }

  @AfterClass
  public static void afterClass() {
    OFileUtils.deleteRecursively(testDir);
  }

  @Test
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

  @Test
  public void testWriteSingleRecord() throws Exception {
    Assert.assertNull(writeAheadLog.end());

    TestRecord writtenRecord = new TestRecord(-1, SEGMENT_SIZE, 30, false, true);
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

  @Test
  public void testFirstMasterRecordUpdate() throws Exception {
    TestRecord writtenRecord = new TestRecord(-1, SEGMENT_SIZE, 30, false, true);

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

  @Test
  public void testSecondMasterRecordUpdate() throws Exception {
    writeAheadLog.log(new TestRecord(-1, SEGMENT_SIZE, 30, false, true));

    writeAheadLog.logFuzzyCheckPointStart(new OLogSequenceNumber(-1, -1));
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.log(new TestRecord(-1, SEGMENT_SIZE, 30, false, true));

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

  @Test
  public void testThirdMasterRecordUpdate() throws Exception {
    writeAheadLog.log(new TestRecord(-1, SEGMENT_SIZE, 30, false, true));

    writeAheadLog.logFuzzyCheckPointStart(new OLogSequenceNumber(-1, -1));
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.log(new TestRecord(-1, SEGMENT_SIZE, 30, false, true));

    writeAheadLog.logFuzzyCheckPointStart(new OLogSequenceNumber(-1, -1));
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.log(new TestRecord(-1, SEGMENT_SIZE, 30, false, true));

    OLogSequenceNumber checkpointLSN = writeAheadLog.logFuzzyCheckPointStart(new OLogSequenceNumber(-1, -1));
    OLogSequenceNumber end = writeAheadLog.logFuzzyCheckPointEnd();

    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), checkpointLSN);
    Assert.assertEquals(writeAheadLog.end(), end);
    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), checkpointLSN);
    Assert.assertEquals(writeAheadLog.end(), end);
  }

  @Test
  public void testWriteMultipleRecordsWithDifferentSizes() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<>();

    Assert.assertEquals(writeAheadLog.size(), 0);

    int recordDistance;
    // first page
    recordDistance = ONE_KB;
    OLogSequenceNumber lsn;
    long duration = 0;
    long nextStart;

    TestRecord walRecord = new TestRecord(-1, SEGMENT_SIZE, recordDistance, false, false);
    lsn = writeAheadLog.log(walRecord);
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);
    duration += recordDistance;

    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), duration);

    recordDistance = ONE_KB;
    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, recordDistance, false, false);
    lsn = writeAheadLog.log(walRecord);
    nextStart = walRecord.nextStart;
    duration += recordDistance;

    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), duration);

    writtenRecords.add(walRecord);

    recordDistance = OWALPage.PAGE_SIZE - 2 * ONE_KB;
    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, recordDistance, false, false);
    lsn = writeAheadLog.log(walRecord);
    nextStart = walRecord.nextStart;
    duration += recordDistance;

    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), duration);

    Assert.assertEquals(writeAheadLog.size(), OWALPage.PAGE_SIZE);
    writtenRecords.add(walRecord);

    // second page
    recordDistance = ONE_KB;
    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, recordDistance, false, false);
    lsn = writeAheadLog.log(walRecord);
    nextStart = walRecord.nextStart;
    duration += ONE_KB;

    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), duration);

    writtenRecords.add(walRecord);

    recordDistance = ONE_KB;
    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, recordDistance, false, false);
    lsn = writeAheadLog.log(walRecord);
    nextStart = walRecord.nextStart;
    duration += ONE_KB;

    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), duration);

    writtenRecords.add(walRecord);

    recordDistance = OWALPage.PAGE_SIZE - 2 * ONE_KB;
    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, recordDistance, false, false);
    lsn = writeAheadLog.log(walRecord);
    duration += recordDistance;
    nextStart = walRecord.nextStart;

    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), duration);

    Assert.assertEquals(writeAheadLog.size(), 2 * OWALPage.PAGE_SIZE);

    writtenRecords.add(walRecord);

    // third page
    recordDistance = ONE_KB;
    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, recordDistance, false, false);
    lsn = writeAheadLog.log(walRecord);
    duration += recordDistance;
    nextStart = walRecord.nextStart;

    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), duration);

    writtenRecords.add(walRecord);

    recordDistance = ONE_KB;
    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, recordDistance, false, false);
    lsn = writeAheadLog.log(walRecord);
    duration += recordDistance;
    nextStart = walRecord.nextStart;

    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), duration);

    writtenRecords.add(walRecord);

    recordDistance = OWALPage.PAGE_SIZE - 2 * ONE_KB;
    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, recordDistance, false, false);
    lsn = writeAheadLog.log(walRecord);
    duration += recordDistance;
    nextStart = walRecord.nextStart;

    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), duration);

    Assert.assertEquals(writeAheadLog.size(), 3 * OWALPage.PAGE_SIZE);
    writtenRecords.add(walRecord);

    // fourth page
    recordDistance = ONE_KB;
    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, recordDistance, false, false);
    lsn = writeAheadLog.log(walRecord);
    duration += recordDistance;
    nextStart = walRecord.nextStart;

    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), duration);

    writtenRecords.add(walRecord);

    recordDistance = ONE_KB;
    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, recordDistance, false, false);
    lsn = writeAheadLog.log(walRecord);
    duration += recordDistance;
    nextStart = walRecord.nextStart;

    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), duration);

    writtenRecords.add(walRecord);

    recordDistance = OWALPage.PAGE_SIZE - 2 * ONE_KB;
    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, recordDistance, false, false);
    lsn = writeAheadLog.log(walRecord);
    duration += recordDistance;
    nextStart = walRecord.nextStart;

    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), duration);

    writtenRecords.add(walRecord);

    // fifth page
    recordDistance = ONE_KB;
    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, recordDistance, false, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    duration += recordDistance;

    writtenRecords.add(walRecord);

    assertLogContent(writeAheadLog, writtenRecords);

    Assert.assertEquals(writeAheadLog.end(), end);
    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.size(), duration);
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), end);

    assertLogContent(writeAheadLog, writtenRecords);
  }

  @Test
  public void testWriteMultipleRecordsWithDifferentSizeAfterCloseOne() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<>();

    long duration;
    long nextStart;

    // first page
    TestRecord walRecord = new TestRecord(-1, SEGMENT_SIZE, ONE_KB, false, false);
    writeAheadLog.log(walRecord);
    duration = ONE_KB;
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, ONE_KB, false, false);
    OLogSequenceNumber lsn = writeAheadLog.log(walRecord);
    nextStart = walRecord.nextStart;
    duration += ONE_KB;
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
      walRecord = new TestRecord(nextStart, SEGMENT_SIZE, recordDistance, false, true);

      end = writeAheadLog.log(walRecord);
      recordDistance = walRecord.distance;

      duration += recordDistance;
      nextStart = walRecord.nextStart;
      writtenRecords.add(walRecord);

      writtenSize += recordDistance;

      assertLogContent(writeAheadLog, writtenRecords);
    }

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.size(), duration);

    assertLogContent(writeAheadLog, writtenRecords);

    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), end);
    Assert.assertEquals(writeAheadLog.size(), duration);

    assertLogContent(writeAheadLog, writtenRecords);
  }

  @Test
  public void testWriteMultipleRecordsWithDifferentSizeAfterCloseTwo() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<>();

    long duration;
    long nextStart;

    // first page
    TestRecord walRecord = new TestRecord(-1, SEGMENT_SIZE, ONE_KB, false, false);
    writeAheadLog.log(walRecord);
    duration = ONE_KB;
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);
    Assert.assertEquals(writeAheadLog.size(), duration);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, ONE_KB, false, false);
    writeAheadLog.log(walRecord);
    duration += ONE_KB;
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);
    Assert.assertEquals(writeAheadLog.size(), duration);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE - 2 * ONE_KB, false, false);
    OLogSequenceNumber lsn = writeAheadLog.log(walRecord);
    duration += OWALPage.PAGE_SIZE - 2 * ONE_KB;
    nextStart = walRecord.nextStart;

    Assert.assertEquals(writeAheadLog.size(), duration);

    writtenRecords.add(walRecord);

    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    long seed = System.currentTimeMillis();
    System.out.println("seed of testWriteMultipleRecordsWithDifferentSizeAfterCloseTwo " + seed);
    Random random = new Random(seed);
    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.size(), duration);

    OLogSequenceNumber end = lsn;
    for (int writtenSize = 0; writtenSize < 4 * OWALPage.PAGE_SIZE; ) {
      int recordDistance = random.nextInt(2 * OWALPage.PAGE_SIZE - 1) + 15;
      walRecord = new TestRecord(nextStart, SEGMENT_SIZE, recordDistance, false, true);

      end = writeAheadLog.log(walRecord);
      recordDistance = walRecord.distance;

      duration += recordDistance;
      nextStart = walRecord.nextStart;
      writtenRecords.add(walRecord);

      Assert.assertEquals(writeAheadLog.size(), duration);
      writtenSize += recordDistance;
    }

    assertLogContent(writeAheadLog, writtenRecords);

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.size(), duration);
    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), end);
    Assert.assertEquals(writeAheadLog.size(), duration);

    assertLogContent(writeAheadLog, writtenRecords);
  }

  @Test
  public void testWriteMultipleRecordsWithDifferentSizeAfterCloseThree() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<>();

    long duration;
    long nextStart;

    // first page
    TestRecord walRecord = new TestRecord(-1, SEGMENT_SIZE, ONE_KB, false, false);
    writeAheadLog.log(walRecord);
    duration = ONE_KB;
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, ONE_KB, false, false);
    writeAheadLog.log(walRecord);
    duration += ONE_KB;
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE - 2 * ONE_KB, false, false);
    OLogSequenceNumber lsn = writeAheadLog.log(walRecord);
    duration += OWALPage.PAGE_SIZE - 2 * ONE_KB;
    nextStart = walRecord.nextStart;
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
      walRecord = new TestRecord(nextStart, SEGMENT_SIZE, recordDistance, false, true);

      end = writeAheadLog.log(walRecord);

      recordDistance = walRecord.distance;
      duration += recordDistance;
      nextStart = walRecord.nextStart;
      writtenRecords.add(walRecord);

      writtenSize += recordDistance;
    }

    assertLogContent(writeAheadLog, writtenRecords);

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.size(), duration);

    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), end);
    Assert.assertEquals(writeAheadLog.size(), duration);

    assertLogContent(writeAheadLog, writtenRecords);
  }

  @Test
  public void testWriteMultipleRecordsWithDifferentSizeAfterCloseFour() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<>();

    long duration;
    long nextStart;

    // first page
    TestRecord walRecord = new TestRecord(-1, SEGMENT_SIZE, ONE_KB, false, false);
    writeAheadLog.log(walRecord);
    duration = ONE_KB;
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, ONE_KB, false, false);
    writeAheadLog.log(walRecord);
    duration += ONE_KB;
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE - 2 * ONE_KB, false, false);
    OLogSequenceNumber lsn = writeAheadLog.log(walRecord);
    nextStart = walRecord.nextStart;
    duration += OWALPage.PAGE_SIZE - 2 * ONE_KB;

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
      walRecord = new TestRecord(nextStart, SEGMENT_SIZE, recordDistance, false, true);

      end = writeAheadLog.log(walRecord);
      recordDistance = walRecord.distance;
      duration += recordDistance;
      nextStart = walRecord.nextStart;
      writtenRecords.add(walRecord);

      writtenSize += recordDistance;
    }

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.size(), duration);
    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), end);
    Assert.assertEquals(writeAheadLog.size(), duration);

    assertLogContent(writeAheadLog, writtenRecords);
  }

  @Test
  public void testWriteMultipleRandomRecords() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<>();

    long seed = System.currentTimeMillis();
    System.out.println("seed of testWriteMultipleRecordsWithDifferentSizeAfterCloseFour " + seed);
    Random random = new Random(seed);

    OLogSequenceNumber lsn = null;
    long duration = 0;
    long nextStart = 0;

    for (int writtenSize = 0; writtenSize < 16 * OWALPage.PAGE_SIZE; ) {
      int recordDistance = random.nextInt(2 * OWALPage.PAGE_SIZE - 1) + OWALPageV2.RECORDS_OFFSET + 10;

      TestRecord walRecord = new TestRecord(nextStart, SEGMENT_SIZE, recordDistance, false, true);

      lsn = writeAheadLog.log(walRecord);
      nextStart = walRecord.nextStart;
      recordDistance = walRecord.distance;

      duration += recordDistance;
      Assert.assertEquals(writeAheadLog.size(), duration);
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

      TestRecord walRecord = new TestRecord(nextStart, SEGMENT_SIZE, recordDistance, false, true);

      end = writeAheadLog.log(walRecord);
      nextStart = walRecord.nextStart;

      recordDistance = walRecord.distance;
      duration += recordDistance;
      Assert.assertEquals(writeAheadLog.size(), duration);
      writtenRecords.add(walRecord);

      writtenSize += recordDistance;
    }

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.size(), duration);

    assertLogContent(writeAheadLog, writtenRecords);

    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), end);
    Assert.assertEquals(writeAheadLog.size(), duration);

    assertLogContent(writeAheadLog, writtenRecords);
  }

  @Test
  public void testCutTillLimits() throws IOException {
    long nextStart = 0;

    for (int n = 0; n < 3; n++) {
      for (int i = 0; i < SEGMENT_SIZE / OWALPage.PAGE_SIZE; i++) {
        TestRecord walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);
        writeAheadLog.log(walRecord);
        nextStart = walRecord.nextStart;
      }

      TestRecord walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);
      writeAheadLog.log(walRecord);
    }

    try {
      writeAheadLog.addCutTillLimit(null);
      Assert.fail();
    } catch (NullPointerException npe) {
      Assert.assertTrue(true);
    }

    try {
      writeAheadLog.removeCutTillLimit(null);
      Assert.fail();
    } catch (NullPointerException npe) {
      Assert.assertTrue(true);
    }

    final OLogSequenceNumber end = writeAheadLog.end();
    final OLogSequenceNumber begin = writeAheadLog.begin();

    writeAheadLog.addCutTillLimit(new OLogSequenceNumber(25, 25));
    writeAheadLog.addCutTillLimit(new OLogSequenceNumber(25, 25));

    writeAheadLog.addCutTillLimit(new OLogSequenceNumber(0, 0));
    writeAheadLog.addCutTillLimit(new OLogSequenceNumber(0, 0));
    writeAheadLog.addCutTillLimit(new OLogSequenceNumber(0, 0));

    writeAheadLog.addCutTillLimit(new OLogSequenceNumber(1, 10));

    writeAheadLog.addCutTillLimit(new OLogSequenceNumber(2, 10));
    writeAheadLog.addCutTillLimit(new OLogSequenceNumber(2, 10));

    writeAheadLog.cutTill(new OLogSequenceNumber(3, 0));

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.begin(), begin);

    writeAheadLog.removeCutTillLimit(new OLogSequenceNumber(0, 0));
    writeAheadLog.cutTill(new OLogSequenceNumber(3, 0));

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.begin(), begin);

    writeAheadLog.removeCutTillLimit(new OLogSequenceNumber(0, 0));
    writeAheadLog.cutTill(new OLogSequenceNumber(3, 0));
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.begin(), begin);

    writeAheadLog.removeCutTillLimit(new OLogSequenceNumber(1, 10));
    writeAheadLog.cutTill(new OLogSequenceNumber(3, 0));
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.begin(), begin);

    writeAheadLog.removeCutTillLimit(new OLogSequenceNumber(0, 0));
    writeAheadLog.cutTill(new OLogSequenceNumber(3, 0));
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.begin().getSegment(), 2);

    writeAheadLog.removeCutTillLimit(new OLogSequenceNumber(2, 10));
    writeAheadLog.cutTill(new OLogSequenceNumber(3, 0));
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.begin().getSegment(), 2);

    try {
      writeAheadLog.removeCutTillLimit(new OLogSequenceNumber(2, 1));
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(true);
    }

    writeAheadLog.removeCutTillLimit(new OLogSequenceNumber(2, 10));
    writeAheadLog.cutTill(new OLogSequenceNumber(3, 0));
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.begin().getSegment(), 3);

    writeAheadLog.removeCutTillLimit(new OLogSequenceNumber(25, 25));
    writeAheadLog.removeCutTillLimit(new OLogSequenceNumber(25, 25));

    try {
      writeAheadLog.removeCutTillLimit(new OLogSequenceNumber(25, 25));
      Assert.fail();
    } catch (IllegalArgumentException iae) {
      Assert.assertTrue(true);
    }

  }

  @Test
  public void testFlushedLSNOnePage() throws Exception {
    long duration;
    long nextStart = 0;

    TestRecord walRecord = new TestRecord(nextStart, SEGMENT_SIZE, ONE_KB, false, false);
    writeAheadLog.log(walRecord);
    duration = ONE_KB;
    nextStart = walRecord.nextStart;

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, ONE_KB, false, false);
    writeAheadLog.log(walRecord);
    duration += ONE_KB;
    nextStart = walRecord.nextStart;

    Assert.assertNull(writeAheadLog.getFlushedLsn());

    writeAheadLog.flush();

    Assert.assertEquals(writeAheadLog.getFlushedLsn(), walRecord.getLsn());
    Assert.assertEquals(writeAheadLog.end(), walRecord.getLsn());
    Assert.assertEquals(writeAheadLog.size(), duration);

    final OLogSequenceNumber end = walRecord.getLsn();

    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), end);
    Assert.assertEquals(writeAheadLog.size(), duration);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, ONE_KB, false, false);
    OLogSequenceNumber lsn = writeAheadLog.log(walRecord);
    duration += ONE_KB;

    Assert.assertEquals(writeAheadLog.getFlushedLsn(), end);
    Assert.assertEquals(end.compareTo(lsn), -1);
    Assert.assertEquals(writeAheadLog.end(), walRecord.getLsn());
    Assert.assertEquals(writeAheadLog.size(), duration);
  }

  @Test
  public void testFlushedLSNOnePageWithLessThanMinRecordSpace() throws Exception {
    long duration;
    long nextStart;

    TestRecord walRecord = new TestRecord(-1, SEGMENT_SIZE, ONE_KB, false, false);
    writeAheadLog.log(walRecord);
    duration = ONE_KB;
    nextStart = walRecord.nextStart;

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, ONE_KB, false, false);
    writeAheadLog.log(walRecord);
    duration += ONE_KB;
    nextStart = walRecord.nextStart;

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE - 2 * ONE_KB, false, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    duration += OWALPage.PAGE_SIZE - 2 * ONE_KB;
    nextStart = walRecord.nextStart;

    Assert.assertNull(writeAheadLog.getFlushedLsn());

    writeAheadLog.flush();

    Assert.assertEquals(writeAheadLog.getFlushedLsn(), end);
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.size(), duration);

    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), walRecord.getLsn());
    Assert.assertEquals(writeAheadLog.size(), duration);

    final OLogSequenceNumber flushedLSN = walRecord.getLsn();

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, ONE_KB, false, false);
    OLogSequenceNumber lsn = writeAheadLog.log(walRecord);
    duration += ONE_KB;

    Assert.assertEquals(writeAheadLog.getFlushedLsn(), flushedLSN);

    Assert.assertEquals(end.compareTo(lsn), -1);
    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), duration);
  }

  @Test
  public void testFlushedLSNOnePageWithMinRecordSpace() throws Exception {
    long duration;
    long nextStart;

    TestRecord walRecord = new TestRecord(-1, SEGMENT_SIZE, ONE_KB, false, false);
    writeAheadLog.log(walRecord);
    duration = ONE_KB;
    nextStart = walRecord.nextStart;

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, ONE_KB, false, false);
    writeAheadLog.log(walRecord);
    duration += ONE_KB;
    nextStart = walRecord.nextStart;

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE - 2 * ONE_KB, false, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    duration += OWALPage.PAGE_SIZE - 2 * ONE_KB;
    nextStart = walRecord.nextStart;

    Assert.assertNull(writeAheadLog.getFlushedLsn());

    writeAheadLog.flush();

    Assert.assertEquals(writeAheadLog.getFlushedLsn(), walRecord.getLsn());
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.size(), duration);

    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), walRecord.getLsn());
    Assert.assertEquals(writeAheadLog.size(), duration);

    end = writeAheadLog.end();

    OLogSequenceNumber flushedLSN = walRecord.getLsn();

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, ONE_KB, false, false);
    OLogSequenceNumber lsn = writeAheadLog.log(walRecord);
    duration += ONE_KB;

    Assert.assertEquals(writeAheadLog.getFlushedLsn(), flushedLSN);
    Assert.assertEquals(end.compareTo(lsn), -1);
    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), duration);
  }

  @Test
  public void testFlushedLSNOnePageWithNoSpace() throws Exception {
    OLogSequenceNumber lsn;
    long duration;
    long nextStart = 0;

    TestRecord walRecord = new TestRecord(nextStart, SEGMENT_SIZE, ONE_KB, false, false);
    writeAheadLog.log(walRecord);
    duration = ONE_KB;
    nextStart = walRecord.nextStart;

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, ONE_KB, false, false);
    writeAheadLog.log(walRecord);
    duration += ONE_KB;
    nextStart = walRecord.nextStart;

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE - 2 * ONE_KB, false, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    duration += OWALPage.PAGE_SIZE - 2 * ONE_KB;
    nextStart = walRecord.nextStart;

    Assert.assertNull(writeAheadLog.getFlushedLsn());

    writeAheadLog.flush();

    Assert.assertEquals(writeAheadLog.getFlushedLsn(), walRecord.getLsn());
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.size(), duration);

    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), walRecord.getLsn());
    Assert.assertEquals(writeAheadLog.size(), duration);

    OLogSequenceNumber flushedLSN = walRecord.getLsn();
    end = writeAheadLog.end();

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, ONE_KB, false, false);
    lsn = writeAheadLog.log(walRecord);
    duration += ONE_KB;

    Assert.assertEquals(writeAheadLog.getFlushedLsn(), flushedLSN);
    Assert.assertEquals(end.compareTo(lsn), -1);
    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), duration);
  }

  @Test
  public void testFlushedLSNTwoPagesAndThenTwo() throws Exception {
    long duration = 0;
    long nextStart = -1;

    TestRecord walRecord = null;
    for (int i = 0; i < 2; i++) {
      walRecord = new TestRecord(nextStart, SEGMENT_SIZE, ONE_KB, false, false);
      writeAheadLog.log(walRecord);
      duration += ONE_KB;
      nextStart = walRecord.nextStart;

      walRecord = new TestRecord(nextStart, SEGMENT_SIZE, ONE_KB, false, false);
      writeAheadLog.log(walRecord);
      duration += ONE_KB;
      nextStart = walRecord.nextStart;

      walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE - 2 * ONE_KB, false, false);
      writeAheadLog.log(walRecord);
      duration += OWALPage.PAGE_SIZE - 2 * ONE_KB;
      nextStart = walRecord.nextStart;
    }

    Assert.assertNull(writeAheadLog.getFlushedLsn());

    writeAheadLog.flush();

    OLogSequenceNumber end = null;
    for (int i = 0; i < 2; i++) {
      walRecord = new TestRecord(nextStart, SEGMENT_SIZE, ONE_KB, false, false);
      writeAheadLog.log(walRecord);
      duration += ONE_KB;
      nextStart = walRecord.nextStart;

      walRecord = new TestRecord(nextStart, SEGMENT_SIZE, ONE_KB, false, false);
      writeAheadLog.log(walRecord);
      duration += ONE_KB;
      nextStart = walRecord.nextStart;

      walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE - 2 * ONE_KB, false, false);
      end = writeAheadLog.log(walRecord);
      duration += OWALPage.PAGE_SIZE - 2 * ONE_KB;
      nextStart = walRecord.nextStart;
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

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, ONE_KB, false, false);
    OLogSequenceNumber lsn = writeAheadLog.log(walRecord);
    duration += ONE_KB;

    Assert.assertEquals(writeAheadLog.getFlushedLsn(), flushedLSN);

    Assert.assertEquals(end.compareTo(lsn), -1);
    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), duration);
  }

  @Test
  public void testFlushedLSNTwoPagesOneWithTrail() throws Exception {
    long duration;
    long nextStart = 0;

    TestRecord walRecord = new TestRecord(nextStart, SEGMENT_SIZE, ONE_KB, false, false);
    writeAheadLog.log(walRecord);
    duration = ONE_KB;
    nextStart = walRecord.nextStart;

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, ONE_KB, false, false);
    writeAheadLog.log(walRecord);
    duration += ONE_KB;
    nextStart = walRecord.nextStart;

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE - 2 * ONE_KB, false, false);
    writeAheadLog.log(walRecord);
    duration += OWALPage.PAGE_SIZE - 2 * ONE_KB;
    nextStart = walRecord.nextStart;

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, ONE_KB, false, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    duration += ONE_KB;
    nextStart = walRecord.nextStart;

    Assert.assertNull(writeAheadLog.getFlushedLsn());

    writeAheadLog.flush();
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), walRecord.getLsn());
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.size(), duration);

    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), walRecord.getLsn());
    Assert.assertEquals(writeAheadLog.size(), duration);

    OLogSequenceNumber flushedLSN = walRecord.getLsn();
    end = writeAheadLog.end();

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, ONE_KB, false, false);
    OLogSequenceNumber lsn = writeAheadLog.log(walRecord);
    duration += ONE_KB;

    Assert.assertEquals(writeAheadLog.getFlushedLsn(), flushedLSN);
    Assert.assertEquals(end.compareTo(lsn), -1);
    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), duration);
  }

  @Test
  public void testFlushedLSNTwoTwoSegments() throws Exception {
    long seek = System.currentTimeMillis();
    System.out.println("testFlushedLSNTwoTwoSegments seek " + seek);
    Random random = new Random(seek);

    int writtenContent = 0;
    TestRecord walRecord;
    long duration = 0;
    long nextStart = 0;

    while (writtenContent <= 4 * OWALPage.PAGE_SIZE) {
      int recordDistance = random.nextInt(OWALPage.PAGE_SIZE - 1) + 15;

      walRecord = new TestRecord(nextStart, SEGMENT_SIZE, recordDistance, false, true);
      writeAheadLog.log(walRecord);

      recordDistance = walRecord.distance;
      duration += recordDistance;
      nextStart = walRecord.nextStart;

      writtenContent += recordDistance;
    }

    int recordDistance = random.nextInt(OWALPage.PAGE_SIZE - 1) + 15;
    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, recordDistance, false, true);
    recordDistance = walRecord.distance;

    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    duration += recordDistance;
    nextStart = walRecord.nextStart;

    writeAheadLog.flush();

    Assert.assertEquals(writeAheadLog.getFlushedLsn(), walRecord.getLsn());
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.size(), duration);

    writeAheadLog.close();

    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), walRecord.getLsn());
    Assert.assertEquals(writeAheadLog.size(), duration);

    end = writeAheadLog.end();
    OLogSequenceNumber flushedLSN = walRecord.getLsn();

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, ONE_KB, false, true);
    OLogSequenceNumber lsn = writeAheadLog.log(walRecord);
    duration += ONE_KB;

    Assert.assertEquals(writeAheadLog.getFlushedLsn(), flushedLSN);
    Assert.assertEquals(writeAheadLog.size(), duration);

    Assert.assertEquals(end.compareTo(lsn), -1);
    Assert.assertEquals(writeAheadLog.end(), lsn);
  }

  @Test
  public void testFirstMasterRecordIsBrokenSingleRecord() throws Exception {
    writeAheadLog.log(new TestRecord(-1, SEGMENT_SIZE, 30, false, true));

    writeAheadLog.logFuzzyCheckPointStart(new OLogSequenceNumber(-1, -1));
    OLogSequenceNumber end = writeAheadLog.logFuzzyCheckPointEnd();

    Assert.assertEquals(writeAheadLog.end(), end);
    writeAheadLog.close();

    RandomAccessFile mrFile = new RandomAccessFile(new File(writeAheadLog.getWalLocation().toFile(), "WriteAheadLogTest.wmr"),
        "rw");
    mrFile.seek(OIntegerSerializer.INT_SIZE + 1);

    int bt = mrFile.read();
    mrFile.seek(OIntegerSerializer.INT_SIZE + 1);
    mrFile.write(bt + 1);
    mrFile.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertNull(writeAheadLog.getLastCheckpoint());
  }

  @Test
  public void testSecondMasterRecordIsBroken() throws Exception {
    writeAheadLog.log(new TestRecord(-1, SEGMENT_SIZE, 30, false, true));

    OLogSequenceNumber checkPointLSN = writeAheadLog.logFuzzyCheckPointStart(new OLogSequenceNumber(-1, -1));
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.log(new TestRecord(-1, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, true));

    writeAheadLog.logFuzzyCheckPointStart(new OLogSequenceNumber(-1, -1));
    OLogSequenceNumber end = writeAheadLog.logFuzzyCheckPointEnd();

    Assert.assertEquals(writeAheadLog.end(), end);
    writeAheadLog.close();

    RandomAccessFile mrFile = new RandomAccessFile(new File(writeAheadLog.getWalLocation().toFile(), "WriteAheadLogTest.wmr"),
        "rw");
    mrFile.seek(3 * OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE);

    int bt = mrFile.read();
    mrFile.seek(3 * OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE);
    mrFile.write(bt + 1);
    mrFile.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), checkPointLSN);
  }

  @Test
  public void testFirstMasterRecordIsBrokenThreeCheckpoints() throws Exception {
    writeAheadLog.log(new TestRecord(-1, SEGMENT_SIZE, 30, false, true));

    writeAheadLog.logFuzzyCheckPointStart(new OLogSequenceNumber(-1, -1));
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.log(new TestRecord(-1, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, true));

    OLogSequenceNumber checkPointLSN = writeAheadLog.logFuzzyCheckPointStart(new OLogSequenceNumber(-1, -1));
    writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.log(new TestRecord(-1, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, true));

    writeAheadLog.logFuzzyCheckPointStart(new OLogSequenceNumber(-1, -1));
    OLogSequenceNumber end = writeAheadLog.logFuzzyCheckPointEnd();

    writeAheadLog.close();

    RandomAccessFile mrFile = new RandomAccessFile(new File(writeAheadLog.getWalLocation().toFile(), "WriteAheadLogTest.wmr"),
        "rw");
    mrFile.seek(0);

    int bt = mrFile.read();
    mrFile.seek(0);
    mrFile.write(bt + 1);
    mrFile.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getLastCheckpoint(), checkPointLSN);
  }

  @Test
  public void testWriteMultipleRecords() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<>();

    final int recordsToWrite = 2048;
    OLogSequenceNumber end = null;
    long nextStart = 0;

    for (int i = 0; i < recordsToWrite; i++) {
      TestRecord setPageDataRecord = new TestRecord(nextStart, SEGMENT_SIZE, 30, false, true);
      writtenRecords.add(setPageDataRecord);

      end = writeAheadLog.log(setPageDataRecord);
      nextStart = setPageDataRecord.nextStart;
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

  @Test
  public void testAppendMultipleRecordsAfterClose() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<>();

    OLogSequenceNumber end = null;
    long distance = 0;
    long nextStart = 0;

    final int recordsToWrite = 1;
    for (int i = 0; i < recordsToWrite; i++) {
      TestRecord testRecord = new TestRecord(nextStart, SEGMENT_SIZE, 65, false, true);
      writtenRecords.add(testRecord);

      end = writeAheadLog.log(testRecord);
      distance += testRecord.distance;
      nextStart = testRecord.nextStart;
    }

    Assert.assertEquals(writeAheadLog.end(), end);
    writeAheadLog.close();
    writeAheadLog = createWAL();

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), end);
    Assert.assertEquals(writeAheadLog.size(), distance);

    for (int i = 0; i < recordsToWrite; i++) {
      TestRecord testRecord = new TestRecord(nextStart, SEGMENT_SIZE, 65, false, true);
      writtenRecords.add(testRecord);

      end = writeAheadLog.log(testRecord);
      distance += testRecord.distance;
      nextStart = testRecord.nextStart;
    }

    assertLogContent(writeAheadLog, writtenRecords);
    assertLogContent(writeAheadLog, writtenRecords.subList(writtenRecords.size() / 2, writtenRecords.size()));

    Assert.assertEquals(writeAheadLog.end(), end);
    writeAheadLog.close();

    writeAheadLog = createWAL();
    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), end);
    Assert.assertEquals(writeAheadLog.size(), distance);

    assertLogContent(writeAheadLog, writtenRecords);
    assertLogContent(writeAheadLog, writtenRecords.subList(writtenRecords.size() / 2, writtenRecords.size()));
  }

  @Test
  public void testPageIsBrokenOnOtherSegment() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<>();

    long nextStart = 0;

    TestRecord walRecord = new TestRecord(nextStart, SEGMENT_SIZE, ONE_KB, false, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);
    writeAheadLog.log(walRecord);
    nextStart = walRecord.nextStart;

    writtenRecords.add(walRecord);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, ONE_KB, false, false);
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

  @Test
  public void testPageIsBrokenThreeSegmentsOneRecordIsTwoPageWide() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<>();

    long nextStart = 0;

    TestRecord walRecord = new TestRecord(nextStart, SEGMENT_SIZE, ONE_KB, false, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, 2 * OWALPage.PAGE_SIZE, false, false);
    writeAheadLog.log(walRecord);
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, ONE_KB, false, false);
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

  @Test
  public void testPageIsBrokenAndEmpty() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<>();

    long nextStart = 0;

    TestRecord walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);
    writeAheadLog.log(walRecord);
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, 3 * OWALPage.PAGE_SIZE, false, false);
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

  @Test
  public void testSecondPageWasTruncated() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<>();

    long nextStart = 0;

    TestRecord walRecord = new TestRecord(nextStart, SEGMENT_SIZE, 100, false, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);
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

  @Test
  public void testThirdPageWasTruncated() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<>();

    long nextStart = 0;

    TestRecord walRecord = new TestRecord(nextStart, SEGMENT_SIZE, 100, false, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, 2 * OWALPage.PAGE_SIZE, false, false);
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

  @Test
  public void testThirdPageCRCWasIncorrect() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<>();

    long nextStart = 0;

    TestRecord walRecord = new TestRecord(nextStart, SEGMENT_SIZE, 100, false, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, 2 * OWALPage.PAGE_SIZE, false, false);
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

  @Test
  public void testFirstPageInFlushWasBroken() throws Exception {
    long nextStart = 0;

    List<OWALRecord> writtenRecords = new ArrayList<>();

    // first flush
    TestRecord walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);
    writeAheadLog.log(walRecord);
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);
    writeAheadLog.log(walRecord);
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    // second flush
    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);
    writeAheadLog.log(walRecord);
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);
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
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testFirstInCompletePageInFlushWasBroken() throws Exception {
    List<OWALRecord> writtenRecords = new ArrayList<>();
    long nextStart = 0;

    // first flush
    TestRecord walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);
    writeAheadLog.log(walRecord);
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE + 100, false, false);
    writeAheadLog.log(walRecord);
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    // second flush
    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE - 100, false, false);
    writeAheadLog.log(walRecord);
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);

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
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testMiddlePageInFlushWasBroken() throws Exception {
    long duration = 0;
    long nextStart = 0;

    writeAheadLog.close();
    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    List<OWALRecord> writtenRecords = new ArrayList<>();

    // first flush
    TestRecord walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);
    writeAheadLog.log(walRecord);
    duration += OWALPage.PAGE_SIZE;
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);
    writeAheadLog.log(walRecord);
    duration += OWALPage.PAGE_SIZE;
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);
    writeAheadLog.flush();

    // second flush
    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);
    writeAheadLog.log(walRecord);
    duration += OWALPage.PAGE_SIZE;
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);
    writeAheadLog.log(walRecord);
    duration += OWALPage.PAGE_SIZE;
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    duration += OWALPage.PAGE_SIZE;
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
    Assert.assertEquals(writeAheadLog.size(), duration);
    try {
      writeAheadLog.read(writtenRecords.get(3).getLsn());
      Assert.fail();
    } catch (OWALPageBrokenException e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testMiddleIncompletePageInFlushWasBroken() throws Exception {
    writeAheadLog.close();
    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    List<OWALRecord> writtenRecords = new ArrayList<>();

    long duration;
    long nextStart = 0;

    // first flush
    TestRecord walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE + 100, false, false);
    writeAheadLog.log(walRecord);
    duration = OWALPage.PAGE_SIZE + 100;
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);
    Assert.assertEquals(writeAheadLog.size(), duration);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);
    writeAheadLog.log(walRecord);
    duration += OWALPage.PAGE_SIZE;
    nextStart = walRecord.nextStart;
    Assert.assertEquals(writeAheadLog.size(), duration);

    writtenRecords.add(walRecord);
    writeAheadLog.flush();

    // second flush
    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);
    writeAheadLog.log(walRecord);
    duration += OWALPage.PAGE_SIZE;
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);
    Assert.assertEquals(writeAheadLog.size(), duration);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE - 100, false, false);
    writeAheadLog.log(walRecord);
    duration += OWALPage.PAGE_SIZE - 100;
    writtenRecords.add(walRecord);
    nextStart = walRecord.nextStart;
    Assert.assertEquals(writeAheadLog.size(), duration);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    duration += OWALPage.PAGE_SIZE;
    writtenRecords.add(walRecord);
    Assert.assertEquals(writeAheadLog.size(), duration);

    writeAheadLog.close();

    RandomAccessFile rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "rw");
    rndFile.seek(3 * OWALPage.PAGE_SIZE);
    int bt = rndFile.read();
    rndFile.seek(3 * OWALPage.PAGE_SIZE);
    rndFile.write(bt + 1);
    rndFile.close();

    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.size(), duration);

    assertLogContent(writeAheadLog, writtenRecords.subList(0, 2));
    try {
      writeAheadLog.read(writtenRecords.get(2).getLsn());
      Assert.fail();
    } catch (OWALPageBrokenException e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testFirstPageWasNotFlushedFirstCase() throws Exception {
    writeAheadLog.close();
    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    List<OWALRecord> writtenRecords = new ArrayList<>();

    long duration;
    long nextStart = 0;

    // first flush
    TestRecord walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);
    writeAheadLog.log(walRecord);
    duration = OWALPage.PAGE_SIZE;
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE + 100, false, false);
    writeAheadLog.log(walRecord);
    duration += OWALPage.PAGE_SIZE + 100;
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);
    writeAheadLog.flush();

    RandomAccessFile rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "r");
    byte[] content = new byte[OWALPage.PAGE_SIZE];
    rndFile.seek(2 * OWALPage.PAGE_SIZE);
    rndFile.readFully(content);
    rndFile.close();

    // second flush
    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE - 100, false, false);
    writeAheadLog.log(walRecord);
    duration += OWALPage.PAGE_SIZE - 100;
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);
    writeAheadLog.log(walRecord);
    duration += OWALPage.PAGE_SIZE;
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    duration += OWALPage.PAGE_SIZE;
    writtenRecords.add(walRecord);

    writeAheadLog.close();

    rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "rw");
    rndFile.seek(2 * OWALPage.PAGE_SIZE);
    rndFile.write(content);
    rndFile.close();

    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.size(), duration);
    assertLogContent(writeAheadLog, writtenRecords.subList(0, 1));

    try {
      writeAheadLog.read(writtenRecords.get(1).getLsn());
      Assert.fail();
    } catch (OWALPageBrokenException e) {
      Assert.assertTrue(true);
    }
  }

  @Test
  public void testFirstPageWasNotFlushedSecondCase() throws Exception {
    writeAheadLog.close();
    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    List<OWALRecord> writtenRecords = new ArrayList<>();

    long nextStart = 0;
    long duration;

    // first flush
    TestRecord walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE - 100, false, false);
    writeAheadLog.log(walRecord);
    duration = OWALPage.PAGE_SIZE - 100;
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    writeAheadLog.flush();

    RandomAccessFile rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "r");
    byte[] content = new byte[OWALPage.PAGE_SIZE];
    rndFile.seek(0);
    rndFile.readFully(content);
    rndFile.close();

    // second flush
    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE + 100, false, false);
    writeAheadLog.log(walRecord);
    duration += OWALPage.PAGE_SIZE + 100;
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE - 100, false, false);
    writeAheadLog.log(walRecord);
    duration += OWALPage.PAGE_SIZE - 100;
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    duration += OWALPage.PAGE_SIZE;
    writtenRecords.add(walRecord);

    writeAheadLog.close();

    rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "rw");
    rndFile.seek(0);
    rndFile.write(content);
    rndFile.close();

    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.size(), duration);

    try {
      writeAheadLog.read(writtenRecords.get(0).getLsn());
      Assert.fail();
    } catch (OWALPageBrokenException e) {
      Assert.assertTrue(true);
    }
  }

  /**
   * We fill several pages in WAL by data, then damage data on few last pages and then repeat process.
   */
  @Test
  public void testAddRecordsBreakPagesAndAddNewOne() throws Exception {
    final long seed = System.currentTimeMillis();

    System.out.println("testAddRecordsBreakPagesAndAddNewOne : " + seed);
    final Random random = new Random(seed);

    //records are contained in WAL
    final List<TestRecord> writtenRecords = new ArrayList<>();

    //Number of pages for each segment in WAL except of currently active one
    //so size of this list is index of currently active WAL segment
    List<Integer> pagesPerSegment = new ArrayList<>();

    //size of currently active segment in bytes
    int currentSize = 0;

    //amount of pages written in all segments except currently active one
    int pagesWrittenInPreviousSegments = 0;

    //amount of pages written in currently active segment
    int pagesWrittenInCurrentSegment = 0;

    //position of next record in currently active segment
    long nextStart = 0;

    for (int n = 0; n < 5; n++) {
      int pagesToWrite = random.nextInt(6) + 2;

      //we limit max size of record to be no more than size of 3 WAL pages
      int maxDistance = Math
          .min((pagesToWrite - pagesWrittenInPreviousSegments - pagesWrittenInCurrentSegment) * OWALPage.PAGE_SIZE,
              3 * OWALPage.PAGE_SIZE);

      while (maxDistance > 0) {
        final int distance = random.nextInt(maxDistance - 1) + 1;

        final TestRecord walRecord = new TestRecord(nextStart, SEGMENT_SIZE, distance, false, true);

        writeAheadLog.log(walRecord);
        writtenRecords.add(walRecord);

        nextStart = walRecord.nextStart;
        currentSize += walRecord.distance;

        Assert.assertEquals(walRecord.lsn.getSegment(), pagesPerSegment.size());

        //we count not only full but also partially written pages
        pagesWrittenInCurrentSegment = (currentSize + OWALPage.PAGE_SIZE - 1) / OWALPage.PAGE_SIZE;

        //next record will be written in new segment
        if (nextStart / SEGMENT_SIZE > 0) {
          pagesPerSegment.add(pagesWrittenInCurrentSegment);
          pagesWrittenInPreviousSegments += pagesWrittenInCurrentSegment;

          currentSize = 0;
          nextStart = 0;
          pagesWrittenInCurrentSegment = 0;
        }

        maxDistance = Math.min((pagesToWrite - pagesWrittenInPreviousSegments - pagesWrittenInCurrentSegment) * OWALPage.PAGE_SIZE,
            3 * OWALPage.PAGE_SIZE);
      }

      assertLogContent(writeAheadLog, writtenRecords);
      Assert.assertEquals(writeAheadLog.end(), writtenRecords.get(writtenRecords.size() - 1).lsn);

      int logSize = 0;
      for (TestRecord record : writtenRecords) {
        logSize += record.distance;
      }

      Assert.assertEquals(writeAheadLog.size(), logSize);

      writeAheadLog.close();

      final int pagesToBreak = random.nextInt(pagesToWrite - 1) + 1;

      //damage WAL pages since the last one
      for (int pageIndexFromEnd = 1; pageIndexFromEnd <= pagesToBreak; pageIndexFromEnd++) {
        int segmentNumber;

        //number of segment which contains data are going to be broken
        segmentNumber = pagesPerSegment.size();

        //check whether page data of which should be broken are placed inside of current segment
        //or we need to jump few segments toward WAL start
        int prevSegmentPageIndex = pageIndexFromEnd - pagesWrittenInCurrentSegment;

        //amount of pages in all segments which lies after segment which contains page data of which should be broken
        //not including amount of pages in segment which contains page with data which are going to be broken
        int pagesSkipped = 0;

        while (prevSegmentPageIndex > 0) {
          if (segmentNumber == pagesPerSegment.size()) {
            pagesSkipped += pagesWrittenInCurrentSegment;
          } else {
            pagesSkipped += pagesPerSegment.get(segmentNumber);
          }

          segmentNumber--;
          prevSegmentPageIndex -= pagesPerSegment.get(segmentNumber);
        }

        int segmentSize;
        if (segmentNumber == pagesPerSegment.size()) {
          segmentSize = pagesWrittenInCurrentSegment;
        } else {
          segmentSize = pagesPerSegment.get(segmentNumber);
        }

        int pageIndex = segmentSize - (pageIndexFromEnd - pagesSkipped);

        RandomAccessFile rndFile = new RandomAccessFile(
            new File(writeAheadLog.getWalLocation().toFile(), "WriteAheadLogTest." + segmentNumber + ".wal"), "rw");

        Assert.assertEquals(rndFile.length(), segmentSize * OWALPage.PAGE_SIZE);

        rndFile.seek(pageIndex * OWALPage.PAGE_SIZE);

        final int bt = rndFile.read();
        rndFile.seek(pageIndex * OWALPage.PAGE_SIZE);
        rndFile.write(bt + 1);
        rndFile.close();

        //remove all records which at least partially are contained in page data of which was broken
        final ListIterator<TestRecord> recordIterator = writtenRecords.listIterator(writtenRecords.size());
        while (recordIterator.hasPrevious()) {
          final TestRecord record = recordIterator.previous();

          final long recordSegment = record.lsn.getSegment();
          final long recordPosition = record.lsn.getPosition();

          final int recordPageStart = (int) (recordPosition / OWALPage.PAGE_SIZE);
          final int recordPageEnd = (int) (record.nextStart - 1) / OWALPage.PAGE_SIZE;

          if (recordSegment == segmentNumber && recordPageStart <= pageIndex && pageIndex <= recordPageEnd) {
            recordIterator.remove();
          } else {
            break;
          }
        }
      }

      writeAheadLog = createWAL();

      if (!writtenRecords.isEmpty()) {
        assertLogContent(writeAheadLog, writtenRecords);
        Assert.assertEquals(writeAheadLog.begin(), writtenRecords.get(0).lsn);
        Assert.assertEquals(writeAheadLog.end(), writtenRecords.get(writtenRecords.size() - 1).lsn);
        Assert.assertEquals(writeAheadLog.getFlushedLsn(), writtenRecords.get(writtenRecords.size() - 1).lsn);

        logSize = 0;
        for (TestRecord record : writtenRecords) {
          logSize += record.distance;
        }

        Assert.assertEquals(writeAheadLog.size(), logSize);

        final TestRecord lastRecord = writtenRecords.get(writtenRecords.size() - 1);

        if (lastRecord.lsn.getSegment() == pagesPerSegment.size())
          nextStart = lastRecord.nextStart;
        else
          nextStart = 0;

      } else {
        Assert.assertEquals(writeAheadLog.begin(), null);
        Assert.assertEquals(writeAheadLog.end(), null);
        Assert.assertEquals(writeAheadLog.getFlushedLsn(), null);

        Assert.assertEquals(writeAheadLog.size(), 0);

        nextStart = 0;
      }

      ArrayList<Integer> newPagesPerSegment = new ArrayList<>(pagesPerSegment.size());

      for (int i = 0; i < pagesPerSegment.size(); i++) {
        newPagesPerSegment.add(0);
      }

      //even if we break single page, record which it contains
      //may be also contained in other pages so not only damaged page will be removed from the log
      //but several neighbors too, as result we need to recalculate amount of pages in all WAL segments
      pagesWrittenInCurrentSegment = 0;
      pagesWrittenInPreviousSegments = 0;

      currentSize = 0;

      for (TestRecord record : writtenRecords) {
        final int recordSegment = (int) record.lsn.getSegment();

        final int recordIndexEnd = (int) ((record.nextStart - 1) / OWALPage.PAGE_SIZE);
        if (recordSegment == newPagesPerSegment.size()) {
          if (pagesWrittenInCurrentSegment < recordIndexEnd + 1)
            pagesWrittenInCurrentSegment = recordIndexEnd + 1;

          currentSize += record.distance;
        } else {
          if (newPagesPerSegment.get(recordSegment) < recordIndexEnd + 1) {
            newPagesPerSegment.set(recordSegment, recordIndexEnd + 1);
          }
        }

      }

      for (int count : newPagesPerSegment) {
        pagesWrittenInPreviousSegments += count;
      }

      pagesPerSegment = newPagesPerSegment;
    }
  }

  @Test
  public void testPageWasNotFullyWritten() throws Exception {
    writeAheadLog.close();
    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    List<OWALRecord> writtenRecords = new ArrayList<>();

    long duration = 0;
    long nextStart = 0;

    // first flush
    TestRecord walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);
    writeAheadLog.log(walRecord);
    duration += OWALPage.PAGE_SIZE;
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);
    writeAheadLog.log(walRecord);
    duration += OWALPage.PAGE_SIZE;
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);
    writeAheadLog.flush();

    // second flush
    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);
    writeAheadLog.log(walRecord);
    duration += OWALPage.PAGE_SIZE;
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    duration += OWALPage.PAGE_SIZE;
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);
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
    Assert.assertEquals(writeAheadLog.size(), duration);
  }

  @Test
  public void testIncompletePageWasNotFullyWritten() throws Exception {
    writeAheadLog.close();
    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    List<OWALRecord> writtenRecords = new ArrayList<>();
    long duration;
    long nextStart = 0;

    // first flush
    TestRecord walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);
    writeAheadLog.log(walRecord);
    duration = OWALPage.PAGE_SIZE;
    writtenRecords.add(walRecord);
    nextStart = walRecord.nextStart;

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);
    writeAheadLog.log(walRecord);
    duration += OWALPage.PAGE_SIZE;
    nextStart = walRecord.nextStart;

    writtenRecords.add(walRecord);
    writeAheadLog.flush();

    // second flush
    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE, false, false);
    OLogSequenceNumber end = writeAheadLog.log(walRecord);
    duration += OWALPage.PAGE_SIZE;
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE + 100, false, false);
    writeAheadLog.log(walRecord);
    nextStart = walRecord.nextStart;
    writtenRecords.add(walRecord);

    walRecord = new TestRecord(nextStart, SEGMENT_SIZE, OWALPage.PAGE_SIZE - 200, false, false);
    writeAheadLog.log(walRecord);
    writtenRecords.add(walRecord);

    writeAheadLog.close();

    RandomAccessFile rndFile = new RandomAccessFile(new File(testDir, "WriteAheadLogTest.0.wal"), "rw");
    rndFile.setLength(rndFile.length() - OWALPage.PAGE_SIZE / 2);
    rndFile.close();

    writeAheadLog = createWAL(3, 6 * OWALPage.PAGE_SIZE);

    Assert.assertEquals(writeAheadLog.end(), end);
    Assert.assertEquals(writeAheadLog.getFlushedLsn(), end);
    Assert.assertEquals(writeAheadLog.size(), duration);
    assertLogContent(writeAheadLog, writtenRecords.subList(0, 3));
  }

  @Test
  public void testTruncateFirstSegment() throws IOException {
    writeAheadLog.close();
    writeAheadLog = createWAL(6, 3 * OWALPage.PAGE_SIZE);
    final long segmentSize = 3 * OWALPage.PAGE_SIZE;

    OLogSequenceNumber lsn;
    long nextStart = 0;

    TestRecord walRecord = new TestRecord(nextStart, segmentSize, OWALPage.PAGE_SIZE, false, false);
    writeAheadLog.log(walRecord);

    walRecord = new TestRecord(nextStart, segmentSize, OWALPage.PAGE_SIZE, false, false);
    writeAheadLog.log(walRecord);
    nextStart = walRecord.nextStart;

    walRecord = new TestRecord(nextStart, segmentSize, 2 * OWALPage.PAGE_SIZE, false, false);
    writeAheadLog.log(walRecord);
    nextStart = walRecord.nextStart;

    walRecord = new TestRecord(nextStart, segmentSize, OWALPage.PAGE_SIZE / 2, false, false);
    lsn = writeAheadLog.log(walRecord);

    writeAheadLog.cutTill(lsn);

    final OLogSequenceNumber startLSN = writeAheadLog.begin();
    Assert.assertEquals(startLSN, lsn);
    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), OWALPage.PAGE_SIZE / 2);
  }

  @Test
  public void testTruncateLastSegment() throws IOException {
    writeAheadLog.close();
    writeAheadLog = createWAL(6, 3 * OWALPage.PAGE_SIZE);

    final long segmentSize = 3 * OWALPage.PAGE_SIZE;

    long nextStart = 0;

    TestRecord walRecord = new TestRecord(nextStart, segmentSize, OWALPage.PAGE_SIZE, false, false);
    writeAheadLog.log(walRecord);
    nextStart = walRecord.nextStart;

    walRecord = new TestRecord(nextStart, segmentSize, OWALPage.PAGE_SIZE, false, false);
    writeAheadLog.log(walRecord);
    nextStart = walRecord.nextStart;

    walRecord = new TestRecord(nextStart, segmentSize, 2 * OWALPage.PAGE_SIZE, false, false);
    writeAheadLog.log(walRecord);
    nextStart = walRecord.nextStart;

    // second segment
    walRecord = new TestRecord(nextStart, segmentSize, OWALPage.PAGE_SIZE, false, false);
    writeAheadLog.log(walRecord);
    nextStart = walRecord.nextStart;

    walRecord = new TestRecord(nextStart, segmentSize, OWALPage.PAGE_SIZE, false, false);
    writeAheadLog.log(walRecord);
    nextStart = walRecord.nextStart;

    walRecord = new TestRecord(nextStart, segmentSize, OWALPage.PAGE_SIZE, false, false);
    writeAheadLog.log(walRecord);
    nextStart = walRecord.nextStart;

    // last segment
    walRecord = new TestRecord(nextStart, segmentSize, OWALPage.PAGE_SIZE / 2, false, false);
    OLogSequenceNumber lsn = writeAheadLog.log(walRecord);

    writeAheadLog.cutTill(lsn);

    final OLogSequenceNumber startLSN = writeAheadLog.begin();
    Assert.assertEquals(startLSN, lsn);
    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(writeAheadLog.size(), OWALPage.PAGE_SIZE / 2);
  }

  @Test
  public void testEmptyWalCannotBeAppended() {
    Assert.assertFalse(writeAheadLog.appendNewSegment());
  }

  @Test
  public void appendIsNotAllowedWithOnGoingOperations() throws Exception {
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    writeAheadLog.logAtomicOperationStartRecord(false, operationUnitId);
    try {
      writeAheadLog.appendNewSegment();
      Assert.fail();
    } catch (OStorageException ose) {
      Assert.assertTrue(true);
    }

    OLogSequenceNumber endLsn = writeAheadLog.logAtomicOperationEndRecord(operationUnitId, false, new OLogSequenceNumber(0, 0),
        Collections.<String, OAtomicOperationMetadata<?>>emptyMap());

    Assert.assertTrue(writeAheadLog.appendNewSegment());

    OLogSequenceNumber lsn = writeAheadLog.log(new TestRecord(0, SEGMENT_SIZE, 100, false, true));

    Assert.assertEquals(writeAheadLog.end(), lsn);
    Assert.assertEquals(endLsn.getSegment() + 1, lsn.getSegment());
  }

  @Test
  public void emptySegmentNoOpOnAppend() throws Exception {
    OLogSequenceNumber lsn = writeAheadLog.log(new TestRecord(0, SEGMENT_SIZE, SEGMENT_SIZE, false, false));

    final List<String> walFiles = writeAheadLog.getWalFiles();
    Assert.assertEquals(2, walFiles.size());

    Assert.assertFalse(writeAheadLog.appendNewSegment());

    Assert.assertEquals(walFiles, writeAheadLog.getWalFiles());
    OLogSequenceNumber appLsn = writeAheadLog.log(new TestRecord(0, 100, SEGMENT_SIZE, true, false));

    Assert.assertEquals(lsn.getSegment() + 1, appLsn.getSegment());

  }

  @Test
  public void testAppendSegment() throws Exception {
    long nextStart = 0;

    for (int i = 0; i < 10; i++) {
      TestRecord walRecord = new TestRecord(nextStart, SEGMENT_SIZE, 512, false, true);
      writeAheadLog.log(walRecord);
    }

    OLogSequenceNumber end = writeAheadLog.end();
    Assert.assertTrue(writeAheadLog.appendNewSegment());

    TestRecord walRecord = new TestRecord(0, SEGMENT_SIZE, 512, false, true);
    writeAheadLog.log(walRecord);

    Assert.assertEquals(writeAheadLog.end(), walRecord.getLsn());
    Assert.assertEquals(end.getSegment() + 1, walRecord.lsn.getSegment());
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
        Assert.assertTrue(true);
      }
  }

  public static final class TestRecord extends OAbstractWALRecord {
    private byte[]  data;
    private boolean updateMasterRecord;
    /**
     * Start position of next record in WAL segment.
     */
    private long    nextStart;

    /**
     * Distance of generated record
     */
    private int distance;

    /**
     * Used for log record deserialization.
     */
    @SuppressWarnings("unused")
    public TestRecord() {
    }

    /**
     * @param startPosition       Position of start of current record in the file
     * @param segmentSize         Maximum size of the single WAL segment
     * @param distance            Required distance in WAL file between the begging of current record and its end. In other word we
     *                            express how many space we can cover in WAL file by writing this record.
     * @param updateMasterRecord  Flag which indicates whether LSN of this record should be stored in WAL master records registry
     * @param approximateDistance Record distance can be increased if record with required distance can not be generated
     */
    public TestRecord(long startPosition, long segmentSize, int distance, boolean updateMasterRecord, boolean approximateDistance) {
      Random random = new Random();

      int finalSize;
      int originalDistance = distance;
      long originalRecordPositionEnd = startPosition;

      while (true) {
        //if end of the last record cross boundary new record will start
        //at the start of the next segment
        if (startPosition < 0) {
          startPosition = 0;
        }

        if (startPosition / segmentSize > 0) {
          startPosition = 0;
        }

        this.nextStart = startPosition + distance;

        //if we add record to a new page, some of the required space will be covered by
        //system information
        if (startPosition % OWALPage.PAGE_SIZE == 0) {
          startPosition += OWALPageV2.RECORDS_OFFSET;
          distance -= OWALPageV2.RECORDS_OFFSET;
        }

        if (distance <= 0) {
          if (!approximateDistance) {
            throw new IllegalArgumentException("Data size for distance " + distance + " can not be calculated");
          } else {
            originalDistance++;
            distance = originalDistance;
            startPosition = originalRecordPositionEnd;
            continue;
          }
        }

        //free space in the page equals top position of end of last record minus page size
        int freeFirstPageSpace = OWALPage.PAGE_SIZE - (int) (startPosition % OWALPage.PAGE_SIZE);

        if (distance <= freeFirstPageSpace) {
          //take in account that despite user data some service data are added in each wal record
          finalSize = OWALPageV2.calculateRecordSize(distance);

          if (finalSize <= 0) {
            if (!approximateDistance) {
              throw new IllegalArgumentException("Data size for distance " + distance + " can not be calculated");
            } else {
              originalDistance++;
              distance = originalDistance;
              startPosition = originalRecordPositionEnd;
              continue;
            }
          }
        } else {
          distance -= freeFirstPageSpace;

          if (freeFirstPageSpace < OWALPage.MIN_RECORD_SIZE) {
            finalSize = 0;
          } else {
            finalSize = OWALPageV2.calculateRecordSize(freeFirstPageSpace);
          }

          final int amountOfFullPieces = distance / OWALPage.PAGE_SIZE;
          distance -= amountOfFullPieces * OWALPage.PAGE_SIZE;

          finalSize += amountOfFullPieces * OWALPageV2.calculateRecordSize(OWALPageV2.MAX_ENTRY_SIZE);

          if (distance > 0) {
            if (distance <= OWALPageV2.RECORDS_OFFSET) {
              if (!approximateDistance) {
                throw new IllegalArgumentException("Data size for distance " + distance + " can not be calculated");
              } else {
                originalDistance++;
                distance = originalDistance;
                startPosition = originalRecordPositionEnd;
                continue;
              }
            }

            distance -= OWALPageV2.RECORDS_OFFSET;
            final int delta = OWALPageV2.calculateRecordSize(distance);

            if (delta <= 0) {
              if (!approximateDistance) {
                throw new IllegalArgumentException("Data size for distance " + distance + " can not be calculated");
              } else {
                originalDistance++;
                distance = originalDistance;
                startPosition = originalRecordPositionEnd;
                continue;
              }
            }

            finalSize += OWALPageV2.calculateRecordSize(distance);
          }
        }

        //we need to subtract serialization overhead (content length), boolean type, wal record type itself
        if (finalSize - OIntegerSerializer.INT_SIZE - 1 - 1 < 1) {
          if (!approximateDistance) {
            throw new IllegalArgumentException("Can not create record with distance " + distance);
          } else {
            originalDistance++;
            distance = originalDistance;
            startPosition = originalRecordPositionEnd;
            continue;
          }
        }

        break;
      }

      this.distance = originalDistance;
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

      //noinspection RedundantIfStatement
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
