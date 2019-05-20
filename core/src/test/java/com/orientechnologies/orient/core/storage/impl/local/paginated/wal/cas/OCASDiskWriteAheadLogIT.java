package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OCheckpointRequestListener;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class OCASDiskWriteAheadLogIT {
  private static Path testDirectory;

  @BeforeClass
  public static void beforeClass() {
    testDirectory = Paths.get(System.getProperty("buildDirectory" + File.separator + "casWALTest",
        "." + File.separator + "target" + File.separator + "casWALTest"));

    OWALRecordsFactory.INSTANCE.registerNewRecord((byte) 127, TestRecord.class);
  }

  @Before
  public void before() {
    OFileUtils.deleteRecursively(testDirectory.toFile());
  }

  @Test
  public void testAddSingleOnePageRecord() throws Exception {
    final int iterations = 10;

    for (int i = 0; i < iterations; i++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      final long seed = System.nanoTime();
      try {
        final Random random = new Random(seed);

        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE,
            Integer.MAX_VALUE, 20, true, Locale.US, -1, -1, 1000, true, false, true, 10);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

        TestRecord walRecord = new TestRecord(random, wal.pageSize(), 1);
        final OLogSequenceNumber lsn = wal.log(walRecord);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), lsn);

        List<OWriteableWALRecord> records = wal.read(lsn, 10);
        Assert.assertEquals(1, records.size());
        TestRecord readRecord = (TestRecord) records.get(0);

        Assert.assertArrayEquals(walRecord.data, readRecord.data);
        Assert.assertEquals(lsn, walRecord.getLsn());
        wal.close();

        wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE, Integer.MAX_VALUE, 20,
            true, Locale.US, -1, -1, 1000, true, false, true, 10);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(2, OCASWALPage.RECORDS_OFFSET));

        wal.flush();

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(2, OCASWALPage.RECORDS_OFFSET));

        records = wal.read(lsn, 10);
        Assert.assertEquals(2, records.size());
        readRecord = (TestRecord) records.get(0);

        Assert.assertArrayEquals(walRecord.data, readRecord.data);
        Assert.assertEquals(lsn, readRecord.getLsn());

        Assert.assertTrue(records.get(1) instanceof OEmptyWALRecord);

        wal.close();

        Thread.sleep(1);

        if (i > 0 && i % 1000 == 0) {
          System.out.printf("%d iterations out of %d were passed\n", i, iterations);
        }
      } catch (Exception | Error e) {
        System.out.println("testAddSingleOnePageRecord : " + seed);
        throw e;
      }
    }
  }

  @Test
  public void testAddSingleRecordSeveralPages() throws Exception {
    final int iterations = 10;
    for (int i = 0; i < iterations; i++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      final long seed = System.nanoTime();
      try {
        final Random random = new Random(seed);

        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE,
            Integer.MAX_VALUE, 20, true, Locale.US, -1, -1, 1000, true, false, true, 10);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

        TestRecord walRecord = new TestRecord(random, 2 * wal.pageSize(), wal.pageSize());
        final OLogSequenceNumber lsn = wal.log(walRecord);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), lsn);

        List<OWriteableWALRecord> records = wal.read(lsn, 10);
        Assert.assertEquals(1, records.size());
        TestRecord readRecord = (TestRecord) records.get(0);

        Assert.assertArrayEquals(walRecord.data, readRecord.data);
        Assert.assertEquals(lsn, walRecord.getLsn());
        wal.close();

        wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE, Integer.MAX_VALUE, 20,
            true, Locale.US, -1, -1, 1000, true, false, true, 10);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(2, OCASWALPage.RECORDS_OFFSET));

        wal.flush();

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(2, OCASWALPage.RECORDS_OFFSET));

        records = wal.read(lsn, 10);
        Assert.assertEquals(2, records.size());
        Assert.assertEquals(lsn, records.get(0).getLsn());
        readRecord = (TestRecord) records.get(0);

        Assert.assertArrayEquals(walRecord.data, readRecord.data);
        Assert.assertEquals(lsn, readRecord.getLsn());

        Assert.assertTrue(records.get(1) instanceof OEmptyWALRecord);

        wal.close();

        Thread.sleep(1);

        if (i > 0 && i % 1000 == 0) {
          System.out.printf("%d iterations out of %d were passed\n", i, iterations);
        }
      } catch (Exception | Error e) {
        System.out.println("testAddSingleRecordSeveralPages : " + seed);
        throw e;
      }
    }
  }

  @Test
  public void testAddFewSmallRecords() throws Exception {
    final int iterations = 10;
    for (int n = 0; n < iterations; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      final long seed = System.nanoTime();
      try {
        final Random random = new Random(seed);

        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE,
            Integer.MAX_VALUE, 20, true, Locale.US, -1, -1, 1000, true, false, true, 10);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

        List<TestRecord> records = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
          final TestRecord walRecord = new TestRecord(random, wal.pageSize(), 1);
          records.add(walRecord);

          OLogSequenceNumber lsn = wal.log(walRecord);
          Assert.assertEquals(walRecord.getLsn(), lsn);

          Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
          Assert.assertEquals(wal.end(), lsn);

        }

        for (int i = 0; i < 5; i++) {
          final List<OWriteableWALRecord> result = wal.read(records.get(i).getLsn(), 10);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i, 5).iterator();

          while (resultIterator.hasNext()) {
            TestRecord record = recordIterator.next();
            TestRecord resultRecord = (TestRecord) resultIterator.next();

            Assert.assertEquals(record.data, resultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }
        }

        wal.close();

        wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE, Integer.MAX_VALUE, 20,
            true, Locale.US, -1, -1, 1000, true, false, true, 10);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(2, OCASWALPage.RECORDS_OFFSET));

        for (int i = 0; i < 5; i++) {
          final List<OWriteableWALRecord> result = wal.read(records.get(i).getLsn(), 10);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i, 5).iterator();

          while (resultIterator.hasNext()) {
            final OWALRecord resultRecord = resultIterator.next();
            if (resultRecord instanceof OEmptyWALRecord) {
              continue;
            }

            TestRecord testResultRecord = (TestRecord) resultRecord;
            TestRecord record = recordIterator.next();

            Assert.assertArrayEquals(record.data, testResultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }
        }

        wal.close();

        Thread.sleep(1);

        if (n > 0 && n % 1000 == 0) {
          System.out.printf("%d iterations out of %d were passed\n", n, iterations);
        }
      } catch (Exception | Error e) {
        System.out.println("testAddFewSmallRecords : " + seed);
        throw e;
      }
    }
  }

  @Test
  public void testNextAddFewSmallRecords() throws Exception {
    final int iterations = 10;

    for (int n = 0; n < iterations; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      final long seed = System.nanoTime();
      try {
        final Random random = new Random(seed);

        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE,
            Integer.MAX_VALUE, 20, true, Locale.US, -1, -1, 1000, true, false, true, 10);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

        List<TestRecord> records = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
          final TestRecord walRecord = new TestRecord(random, wal.pageSize(), 1);
          records.add(walRecord);

          OLogSequenceNumber lsn = wal.log(walRecord);

          Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
          Assert.assertEquals(wal.end(), lsn);
          Assert.assertEquals(walRecord.getLsn(), lsn);
        }

        for (int i = 0; i < 4; i++) {
          final List<OWriteableWALRecord> result = wal.next(records.get(i).getLsn(), 10);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i + 1, 5).iterator();

          while (resultIterator.hasNext()) {
            TestRecord record = recordIterator.next();
            TestRecord resultRecord = (TestRecord) resultIterator.next();

            Assert.assertArrayEquals(record.data, resultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }
        }

        Assert.assertTrue(wal.next(records.get(4).getLsn(), 10).isEmpty());

        wal.close();

        wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE, Integer.MAX_VALUE, 20,
            true, Locale.US, -1, -1, 1000, true, false, true, 10);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(2, OCASWALPage.RECORDS_OFFSET));

        for (int i = 0; i < 4; i++) {
          final List<OWriteableWALRecord> result = wal.next(records.get(i).getLsn(), 10);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i + 1, 5).iterator();

          while (resultIterator.hasNext()) {
            OWriteableWALRecord resultRecord = resultIterator.next();
            if (resultRecord instanceof OEmptyWALRecord) {
              continue;
            }

            TestRecord testResultRecord = (TestRecord) resultRecord;
            TestRecord record = recordIterator.next();

            Assert.assertArrayEquals(record.data, testResultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }
        }

        List<OWriteableWALRecord> lastResult = wal.next(records.get(4).getLsn(), 10);
        Assert.assertEquals(lastResult.size(), 1);
        OWriteableWALRecord emptyRecord = lastResult.get(0);

        Assert.assertTrue(emptyRecord instanceof OEmptyWALRecord);

        wal.close();

        Thread.sleep(2);

        if (n > 0 && n % 1000 == 0) {
          System.out.printf("%d iterations out of %d were passed\n", n, iterations);
        }
      } catch (Exception | Error e) {
        System.out.println("testNextAddFewSmallRecords : " + seed);
        throw e;
      }
    }
  }

  @Test
  public void testAddFewBigRecords() throws Exception {
    final int iterations = 10;

    for (int n = 0; n < iterations; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      final long seed = System.nanoTime();
      try {
        final Random random = new Random(seed);

        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE,
            Integer.MAX_VALUE, 20, true, Locale.US, -1, -1, 1000, true, false, true, 10);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

        List<TestRecord> records = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
          final TestRecord walRecord = new TestRecord(random, 2 * wal.pageSize(), wal.pageSize());
          records.add(walRecord);

          OLogSequenceNumber lsn = wal.log(walRecord);
          Assert.assertEquals(walRecord.getLsn(), lsn);

          Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
          Assert.assertEquals(wal.end(), lsn);
        }

        for (int i = 0; i < 5; i++) {
          final List<OWriteableWALRecord> result = wal.read(records.get(i).getLsn(), 10);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i, 5).iterator();

          while (resultIterator.hasNext()) {
            TestRecord record = recordIterator.next();
            TestRecord resultRecord = (TestRecord) resultIterator.next();

            Assert.assertArrayEquals(record.data, resultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }
        }

        wal.close();

        wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE, Integer.MAX_VALUE, 20,
            true, Locale.US, -1, -1, 1000, true, false, true, 10);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(2, OCASWALPage.RECORDS_OFFSET));

        for (int i = 0; i < 5; i++) {
          final List<OWriteableWALRecord> result = wal.read(records.get(i).getLsn(), 10);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i, 5).iterator();

          while (resultIterator.hasNext()) {
            OWriteableWALRecord resultRecord = resultIterator.next();
            if (resultRecord instanceof OEmptyWALRecord) {
              continue;
            }
            TestRecord record = recordIterator.next();

            final TestRecord testResultRecord = (TestRecord) resultRecord;
            Assert.assertArrayEquals(record.data, testResultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }
        }

        wal.close();

        Thread.sleep(1);

        if (n > 0 && n % 1000 == 0) {
          System.out.printf("%d iterations out of %d were passed\n", n, iterations);
        }
      } catch (Exception | Error e) {
        System.out.println("testAddFewBigRecords : " + seed);
        throw e;
      }
    }
  }

  @Test
  public void testNextAddFewBigRecords() throws Exception {
    final int iterations = 10;

    for (int n = 0; n < iterations; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      final long seed = System.nanoTime();
      try {
        final Random random = new Random(seed);

        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE,
            Integer.MAX_VALUE, 20, true, Locale.US, -1, -1, 1000, true, false, true, 10);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

        List<TestRecord> records = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
          final TestRecord walRecord = new TestRecord(random, 2 * wal.pageSize(), wal.pageSize());
          records.add(walRecord);

          OLogSequenceNumber lsn = wal.log(walRecord);
          Assert.assertEquals(walRecord.getLsn(), lsn);

          Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
          Assert.assertEquals(wal.end(), lsn);
        }

        for (int i = 0; i < 4; i++) {
          final List<OWriteableWALRecord> result = wal.next(records.get(i).getLsn(), 10);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i + 1, 5).iterator();

          while (resultIterator.hasNext()) {
            TestRecord record = recordIterator.next();
            TestRecord resultRecord = (TestRecord) resultIterator.next();

            Assert.assertArrayEquals(record.data, resultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }
        }

        Assert.assertTrue(wal.next(records.get(4).getLsn(), 10).isEmpty());
        wal.close();

        wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE, Integer.MAX_VALUE, 20,
            true, Locale.US, -1, -1, 1000, true, false, true, 10);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(2, OCASWALPage.RECORDS_OFFSET));

        for (int i = 0; i < 4; i++) {
          final List<OWriteableWALRecord> result = wal.next(records.get(i).getLsn(), 10);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i + 1, 5).iterator();

          while (resultIterator.hasNext()) {
            OWALRecord resultRecord = resultIterator.next();
            if (resultRecord instanceof OEmptyWALRecord) {
              continue;
            }

            TestRecord record = recordIterator.next();
            TestRecord testResultRecord = (TestRecord) resultRecord;

            Assert.assertArrayEquals(record.data, testResultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }
        }

        List<OWriteableWALRecord> lastResult = wal.next(records.get(4).getLsn(), 10);
        Assert.assertEquals(lastResult.size(), 1);
        OWriteableWALRecord emptyRecord = lastResult.get(0);

        Assert.assertTrue(emptyRecord instanceof OEmptyWALRecord);

        wal.close();

        Thread.sleep(2);

        if (n > 0 && n % 1000 == 0) {
          System.out.printf("%d iterations out of %d were passed\n", n, iterations);
        }
      } catch (Exception | Error e) {
        System.out.println("testNextAddFewBigRecords : " + seed);
        throw e;
      }
    }
  }

  @Test
  public void testAddNSmallRecords() throws Exception {
    final int iterations = 1;

    for (int n = 0; n < iterations; n++) {
      final long seed = System.nanoTime();

      OFileUtils.deleteRecursively(testDirectory.toFile());
      try {
        final Random random = new Random(seed);

        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE,
            Integer.MAX_VALUE, 20, true, Locale.US, -1, -1, 1000, true, false, true, 10);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

        List<TestRecord> records = new ArrayList<>();

        final int recordsCount = 10_000;

        for (int i = 0; i < recordsCount; i++) {
          final TestRecord walRecord = new TestRecord(random, wal.pageSize(), 1);
          records.add(walRecord);

          OLogSequenceNumber lsn = wal.log(walRecord);
          Assert.assertEquals(walRecord.getLsn(), lsn);

          Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
          Assert.assertEquals(wal.end(), lsn);
        }

        for (int i = 0; i < recordsCount; i++) {
          final List<OWriteableWALRecord> result = wal.read(records.get(i).getLsn(), 500);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i, recordsCount).iterator();

          while (resultIterator.hasNext()) {
            OWriteableWALRecord resultRecord = resultIterator.next();
            if (resultRecord instanceof OEmptyWALRecord) {
              continue;
            }

            TestRecord record = recordIterator.next();
            TestRecord testResultRecord = (TestRecord) resultRecord;

            Assert.assertArrayEquals(record.data, testResultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }
        }

        wal.close();

        wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE, Integer.MAX_VALUE, 20,
            true, Locale.US, -1, -1, 1000, true, false, true, 10);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(2, OCASWALPage.RECORDS_OFFSET));

        for (int i = 0; i < recordsCount; i++) {
          final List<OWriteableWALRecord> result = wal.read(records.get(i).getLsn(), 500);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i, recordsCount).iterator();

          while (resultIterator.hasNext()) {
            OWriteableWALRecord resultRecord = resultIterator.next();
            if (resultRecord instanceof OEmptyWALRecord) {
              continue;
            }
            TestRecord record = recordIterator.next();

            TestRecord testResultRecord = (TestRecord) resultRecord;

            Assert.assertArrayEquals(record.data, testResultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }
        }

        wal.close();
        Thread.sleep(1);

        System.out.printf("%d iterations out of %d were passed\n", n, iterations);

      } catch (Exception | Error e) {
        System.out.println("testAddNSmallRecords : " + seed);
        throw e;
      }
    }
  }

  @Test
  public void testNextAddNSmallRecords() throws Exception {
    final int iterations = 1;

    for (int n = 0; n < iterations; n++) {
      final long seed = System.nanoTime();

      OFileUtils.deleteRecursively(testDirectory.toFile());
      try {
        final Random random = new Random(seed);

        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE,
            Integer.MAX_VALUE, 20, true, Locale.US, -1, -1, 1000, true, false, true, 10);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

        List<TestRecord> records = new ArrayList<>();

        final int recordsCount = 10_000;

        for (int i = 0; i < recordsCount; i++) {
          final TestRecord walRecord = new TestRecord(random, wal.pageSize(), 1);
          records.add(walRecord);

          OLogSequenceNumber lsn = wal.log(walRecord);
          Assert.assertEquals(walRecord.getLsn(), lsn);

          Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
          Assert.assertEquals(wal.end(), lsn);
        }

        for (int i = 0; i < recordsCount - 1; i++) {
          final List<OWriteableWALRecord> result = wal.next(records.get(i).getLsn(), 500);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i + 1, recordsCount).iterator();

          while (resultIterator.hasNext()) {
            TestRecord record = recordIterator.next();
            TestRecord resultRecord = (TestRecord) resultIterator.next();

            Assert.assertArrayEquals(record.data, resultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }
        }

        Assert.assertTrue(wal.next(records.get(recordsCount - 1).getLsn(), 500).isEmpty());

        wal.close();

        wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE, Integer.MAX_VALUE, 20,
            true, Locale.US, -1, -1, 1000, true, false, true, 10);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(2, OCASWALPage.RECORDS_OFFSET));

        for (int i = 0; i < recordsCount - 1; i++) {
          final List<OWriteableWALRecord> result = wal.next(records.get(i).getLsn(), 500);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i + 1, recordsCount).iterator();

          while (resultIterator.hasNext() && recordIterator.hasNext()) {
            TestRecord record = recordIterator.next();
            TestRecord resultRecord = (TestRecord) resultIterator.next();

            Assert.assertArrayEquals(record.data, resultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }
        }

        List<OWriteableWALRecord> lastResult = wal.next(records.get(recordsCount - 1).getLsn(), 10);
        Assert.assertEquals(lastResult.size(), 1);
        OWriteableWALRecord emptyRecord = lastResult.get(0);

        Assert.assertTrue(emptyRecord instanceof OEmptyWALRecord);

        wal.close();
        Thread.sleep(2);

        System.out.printf("%d iterations out of %d were passed\n", n, iterations);

      } catch (Exception | Error e) {
        System.out.println("testNextAddNSmallRecords : " + seed);
        throw e;
      }
    }
  }

  @Test
  public void testAddNSegments() throws Exception {
    int iterations = 1;

    for (int n = 0; n < iterations; n++) {
      final long seed = System.nanoTime();
      final Random random = new Random(seed);

      OFileUtils.deleteRecursively(testDirectory.toFile());
      try {
        final int numberOfSegmentsToAdd = random.nextInt(4) + 3;

        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE,
            Integer.MAX_VALUE, 20, true, Locale.US, -1, -1, 1000, true, false, true, 10);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

        List<TestRecord> records = new ArrayList<>();

        OLogSequenceNumber lastLsn = null;
        for (int i = 0; i < numberOfSegmentsToAdd; i++) {
          wal.appendNewSegment();

          Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
          if (lastLsn == null) {
            Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
          } else {
            Assert.assertEquals(wal.end(), lastLsn);
          }

          final int recordsCount = random.nextInt(10_000) + 100;
          for (int k = 0; k < recordsCount; k++) {
            final TestRecord walRecord = new TestRecord(random, 2 * wal.pageSize(), wal.pageSize());
            lastLsn = wal.log(walRecord);

            records.add(walRecord);

            Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
            Assert.assertEquals(wal.end(), lastLsn);

          }
        }

        Assert.assertEquals(numberOfSegmentsToAdd + 1, wal.activeSegment());

        for (int i = 0; i < records.size(); i++) {
          final TestRecord testRecord = records.get(i);
          final List<OWriteableWALRecord> result = wal.read(testRecord.getLsn(), 10);

          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i, records.size()).iterator();

          while (resultIterator.hasNext()) {
            TestRecord record = recordIterator.next();
            TestRecord resultRecord = (TestRecord) resultIterator.next();

            Assert.assertArrayEquals(record.data, resultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }
        }

        wal.close();

        long walSize = Files.walk(testDirectory).filter(p -> p.toFile().isFile() && p.getFileName().toString().endsWith(".wal"))
            .mapToLong(p -> p.toFile().length()).sum();

        long calculatedWalSize = ((wal.size() + wal.pageSize() - 1) / wal.pageSize()) * wal.pageSize();

        Assert.assertEquals(calculatedWalSize, walSize);

        wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE, Integer.MAX_VALUE, 20,
            true, Locale.US, -1, -1, 1000, true, false, true, 10);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(numberOfSegmentsToAdd + 2, OCASWALPage.RECORDS_OFFSET));

        Assert.assertEquals(numberOfSegmentsToAdd + 2, wal.activeSegment());

        for (int i = 0; i < records.size(); i++) {
          final TestRecord testRecord = records.get(i);
          final List<OWriteableWALRecord> result = wal.read(testRecord.getLsn(), 10);

          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i, records.size()).iterator();

          while (resultIterator.hasNext() && recordIterator.hasNext()) {
            TestRecord record = recordIterator.next();
            TestRecord resultRecord = (TestRecord) resultIterator.next();

            Assert.assertArrayEquals(record.data, resultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }
        }

        final int recordsCount = random.nextInt(10_000) + 100;
        for (int k = 0; k < recordsCount; k++) {
          final TestRecord walRecord = new TestRecord(random, 2 * wal.pageSize(), wal.pageSize());
          wal.log(walRecord);

          records.add(walRecord);

          Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
          Assert.assertEquals(wal.end(), walRecord.getLsn());
        }

        for (int i = 0; i < records.size(); i++) {
          final TestRecord testRecord = records.get(i);
          final List<OWriteableWALRecord> result = wal.read(testRecord.getLsn(), 10);

          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i, records.size()).iterator();

          while (resultIterator.hasNext()) {
            OWriteableWALRecord writeableRecord = resultIterator.next();
            if (writeableRecord instanceof OEmptyWALRecord) {
              continue;
            }

            TestRecord record = recordIterator.next();
            TestRecord resultRecord = (TestRecord) writeableRecord;

            Assert.assertArrayEquals(record.data, resultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }
        }

        wal.close();

        walSize = Files.walk(testDirectory).filter(p -> p.toFile().isFile() && p.getFileName().toString().endsWith(".wal"))
            .mapToLong(p -> p.toFile().length()).sum();

        calculatedWalSize = ((wal.size() + wal.pageSize() - 1) / wal.pageSize()) * wal.pageSize();

        Assert.assertEquals(calculatedWalSize, walSize);

        Thread.sleep(2);

        System.out.printf("%d iterations out of %d were passed\n", n, iterations);
      } catch (Exception | Error e) {
        System.out.println("testAddNSegments seed : " + seed);
        throw e;
      }
    }
  }

  @Test
  public void testAddNBigRecords() throws Exception {
    final int iterations = 1;

    for (int n = 0; n < iterations; n++) {
      final long seed = System.nanoTime();

      OFileUtils.deleteRecursively(testDirectory.toFile());

      try {
        final Random random = new Random(seed);

        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE,
            Integer.MAX_VALUE, 20, true, Locale.US, -1, -1, 1000, true, false, true, 10);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

        List<TestRecord> records = new ArrayList<>();

        final int recordsCount = 10_000;

        for (int i = 0; i < recordsCount; i++) {
          final TestRecord walRecord = new TestRecord(random, 2 * wal.pageSize(), wal.pageSize());
          records.add(walRecord);

          OLogSequenceNumber lsn = wal.log(walRecord);
          Assert.assertEquals(walRecord.getLsn(), lsn);

          Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
          Assert.assertEquals(wal.end(), lsn);
        }

        for (int i = 0; i < recordsCount; i++) {
          final List<OWriteableWALRecord> result = wal.read(records.get(i).getLsn(), 500);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i, recordsCount).iterator();

          while (resultIterator.hasNext()) {
            TestRecord record = recordIterator.next();
            TestRecord resultRecord = (TestRecord) resultIterator.next();

            Assert.assertArrayEquals(record.data, resultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }
        }

        wal.close();

        wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE, Integer.MAX_VALUE, 20,
            true, Locale.US, -1, -1, 1000, true, false, true, 10);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(2, OCASWALPage.RECORDS_OFFSET));

        for (int i = 0; i < recordsCount; i++) {
          final List<OWriteableWALRecord> result = wal.read(records.get(i).getLsn(), 500);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i, recordsCount).iterator();

          while (resultIterator.hasNext() && recordIterator.hasNext()) {
            TestRecord record = recordIterator.next();
            TestRecord resultRecord = (TestRecord) resultIterator.next();

            Assert.assertArrayEquals(record.data, resultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }
        }

        wal.close();

        Thread.sleep(1);
        System.out.printf("%d iterations out of %d were passed\n", n, iterations);
      } catch (Exception | Error e) {
        System.out.println("testAddNBigRecords : " + seed);
        throw e;
      }
    }
  }

  @Test
  public void testNextAddNBigRecords() throws Exception {
    final int iterations = 1;

    for (int n = 0; n < iterations; n++) {
      final long seed = System.nanoTime();

      OFileUtils.deleteRecursively(testDirectory.toFile());

      try {
        final Random random = new Random(seed);

        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE,
            Integer.MAX_VALUE, 20, true, Locale.US, -1, -1, 1000, true, false, true, 10);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

        List<TestRecord> records = new ArrayList<>();

        final int recordsCount = 10_000;

        for (int i = 0; i < recordsCount; i++) {
          final TestRecord walRecord = new TestRecord(random, 2 * wal.pageSize(), wal.pageSize());
          records.add(walRecord);

          OLogSequenceNumber lsn = wal.log(walRecord);
          Assert.assertEquals(walRecord.getLsn(), lsn);

          Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
          Assert.assertEquals(wal.end(), lsn);
        }

        for (int i = 0; i < recordsCount - 1; i++) {
          final List<OWriteableWALRecord> result = wal.next(records.get(i).getLsn(), 500);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i + 1, recordsCount).iterator();

          while (resultIterator.hasNext()) {
            TestRecord record = recordIterator.next();
            TestRecord resultRecord = (TestRecord) resultIterator.next();

            Assert.assertArrayEquals(record.data, resultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }
        }

        Assert.assertTrue(wal.next(records.get(recordsCount - 1).getLsn(), 500).isEmpty());

        wal.close();

        wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE, Integer.MAX_VALUE, 20,
            true, Locale.US, -1, -1, 1000, true, false, true, 10);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(2, OCASWALPage.RECORDS_OFFSET));

        for (int i = 0; i < recordsCount - 1; i++) {
          final List<OWriteableWALRecord> result = wal.next(records.get(i).getLsn(), 500);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i + 1, recordsCount).iterator();

          while (resultIterator.hasNext() && recordIterator.hasNext()) {
            TestRecord record = recordIterator.next();
            TestRecord resultRecord = (TestRecord) resultIterator.next();

            Assert.assertArrayEquals(record.data, resultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }
        }

        List<OWriteableWALRecord> lastResult = wal.next(records.get(recordsCount - 1).getLsn(), 10);
        Assert.assertEquals(lastResult.size(), 1);
        OWriteableWALRecord emptyRecord = lastResult.get(0);

        Assert.assertTrue(emptyRecord instanceof OEmptyWALRecord);

        wal.close();

        Thread.sleep(2);
        System.out.printf("%d iterations out of %d were passed\n", n, iterations);
      } catch (Exception | Error e) {
        System.out.println("testNextAddNBigRecords : " + seed);
        throw e;
      }
    }
  }

  @Test
  public void testAddRecordsMix() throws Exception {
    final int iterations = 1;
    for (int n = 0; n < iterations; n++) {
      final long seed = System.nanoTime();

      OFileUtils.deleteRecursively(testDirectory.toFile());

      try {
        final Random random = new Random(seed);

        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE,
            Integer.MAX_VALUE, 20, true, Locale.US, -1, -1, 1000, true, false, true, 10);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

        List<TestRecord> records = new ArrayList<>();

        final int recordsCount = 10_000;

        for (int i = 0; i < recordsCount; i++) {
          final TestRecord walRecord = new TestRecord(random, 3 * wal.pageSize(), 1);
          records.add(walRecord);

          OLogSequenceNumber lsn = wal.log(walRecord);
          Assert.assertEquals(walRecord.getLsn(), lsn);

          Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
          Assert.assertEquals(wal.end(), lsn);
        }

        for (int i = 0; i < recordsCount; i++) {
          final List<OWriteableWALRecord> result = wal.read(records.get(i).getLsn(), 500);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i, recordsCount).iterator();

          while (resultIterator.hasNext()) {
            TestRecord record = recordIterator.next();
            TestRecord resultRecord = (TestRecord) resultIterator.next();

            Assert.assertArrayEquals(record.data, resultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }
        }

        wal.close();

        wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE, Integer.MAX_VALUE, 20,
            true, Locale.US, -1, -1, 1000, true, false, true, 10);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(2, OCASWALPage.RECORDS_OFFSET));

        for (int i = 0; i < recordsCount; i++) {
          final List<OWriteableWALRecord> result = wal.read(records.get(i).getLsn(), 500);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i, recordsCount).iterator();

          while (resultIterator.hasNext() && recordIterator.hasNext()) {
            TestRecord record = recordIterator.next();
            TestRecord resultRecord = (TestRecord) resultIterator.next();

            Assert.assertArrayEquals(record.data, resultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }
        }

        wal.close();

        System.out.printf("%d iterations out of %d were passed\n", n, iterations);
      } catch (Exception | Error e) {
        System.out.println("testAddRecordsMix : " + seed);
        throw e;
      }
    }
  }

  @Test
  public void testNextAddRecordsMix() throws Exception {
    final int iterations = 1;
    for (int n = 0; n < iterations; n++) {
      final long seed = System.nanoTime();

      OFileUtils.deleteRecursively(testDirectory.toFile());

      try {
        final Random random = new Random(seed);

        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE,
            Integer.MAX_VALUE, 20, true, Locale.US, -1, -1, 1000, true, false, true, 10);
        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

        List<TestRecord> records = new ArrayList<>();

        final int recordsCount = 10_000;

        for (int i = 0; i < recordsCount; i++) {
          final TestRecord walRecord = new TestRecord(random, 3 * wal.pageSize(), 1);
          records.add(walRecord);

          OLogSequenceNumber lsn = wal.log(walRecord);
          Assert.assertEquals(walRecord.getLsn(), lsn);

          Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
          Assert.assertEquals(wal.end(), lsn);
        }

        for (int i = 0; i < recordsCount - 1; i++) {
          final List<OWriteableWALRecord> result = wal.next(records.get(i).getLsn(), 500);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i + 1, recordsCount).iterator();

          while (resultIterator.hasNext()) {
            TestRecord record = recordIterator.next();
            TestRecord resultRecord = (TestRecord) resultIterator.next();

            Assert.assertArrayEquals(record.data, resultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }
        }

        Assert.assertTrue(wal.next(records.get(recordsCount - 1).getLsn(), 500).isEmpty());

        wal.close();

        wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE, Integer.MAX_VALUE, 20,
            true, Locale.US, -1, -1, 1000, true, false, true, 10);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(2, OCASWALPage.RECORDS_OFFSET));

        for (int i = 0; i < recordsCount - 1; i++) {
          final List<OWriteableWALRecord> result = wal.next(records.get(i).getLsn(), 500);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i + 1, recordsCount).iterator();

          while (resultIterator.hasNext() && recordIterator.hasNext()) {
            TestRecord record = recordIterator.next();
            TestRecord resultRecord = (TestRecord) resultIterator.next();

            Assert.assertArrayEquals(record.data, resultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }
        }

        List<OWriteableWALRecord> lastResult = wal.next(records.get(recordsCount - 1).getLsn(), 10);
        Assert.assertEquals(lastResult.size(), 1);
        OWriteableWALRecord emptyRecord = lastResult.get(0);

        Assert.assertTrue(emptyRecord instanceof OEmptyWALRecord);

        wal.close();

        Thread.sleep(2);
        System.out.printf("%d iterations out of %d were passed\n", n, iterations);
      } catch (Exception | Error e) {
        System.out.println("testNextAddRecordsMix : " + seed);
        throw e;
      }
    }
  }

  @Test
  public void testSegSize() throws Exception {
    final int iterations = 1;

    for (int n = 0; n < iterations; n++) {
      final long seed = System.nanoTime();
      OFileUtils.deleteRecursively(testDirectory.toFile());

      try {
        final Random random = new Random(seed);
        final int recordsCount = random.nextInt(10_000) + 100;

        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE,
            Integer.MAX_VALUE, 20, true, Locale.US, -1, -1, 1000, true, false, true, 10);
        for (int i = 0; i < recordsCount; i++) {

          final TestRecord testRecord = new TestRecord(random, 4 * wal.pageSize(), 1);
          wal.log(testRecord);
        }

        wal.close();

        final long segSize = Files.walk(testDirectory)
            .filter(p -> p.toFile().isFile() && p.getFileName().toString().endsWith(".wal")).mapToLong(p -> p.toFile().length())
            .sum();

        final long calculatedSegSize = ((wal.segSize() + wal.pageSize() - 1) / wal.pageSize()) * wal.pageSize();
        Assert.assertEquals(segSize, calculatedSegSize);

        Thread.sleep(2);

        if (n > 0 && n % 10 == 0) {
          System.out.printf("%d iterations out of %d were passed\n", n, iterations);
        }
      } catch (Exception | Error e) {
        System.out.println("testLogSize : " + seed);
        throw e;
      }
    }
  }

  @Test
  @Ignore
  public void appendMT10MSegSmallCacheTest() throws Exception {
    final int iterations = 240;
    for (int n = 0; n < iterations; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE,
          10 * 1024 * 1024, 20, true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

      Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

      AtomicBoolean walIsFull = new AtomicBoolean();

      final OCheckpointRequestListener checkpointRequestListener = () -> walIsFull.set(true);
      wal.addFullCheckpointListener(checkpointRequestListener);
      ExecutorService executorService = Executors.newCachedThreadPool();

      AtomicReference<Future<Void>> segmentAppender = new AtomicReference<>();

      final OSegmentOverflowListener listener = (segment) -> {
        Future<Void> oldAppender = segmentAppender.get();

        while (oldAppender == null || oldAppender.isDone()) {
          if (wal.activeSegment() <= segment) {
            final Future<Void> appender = executorService.submit(new SegmentAdder(segment, wal));

            if (segmentAppender.compareAndSet(oldAppender, appender)) {
              break;
            }

            appender.cancel(false);
            oldAppender = segmentAppender.get();
          } else {
            break;
          }
        }
      };

      wal.addSegmentOverflowListener(listener);

      final TreeMap<OLogSequenceNumber, TestRecord> addedRecords = new TreeMap<>();
      final List<Future<List<TestRecord>>> futures = new ArrayList<>();

      for (int i = 0; i < 8; i++) {
        futures.add(executorService.submit(new RecordsAdder(wal, walIsFull)));
      }

      for (Future<List<TestRecord>> future : futures) {
        final List<TestRecord> records = future.get();
        for (TestRecord record : records) {
          addedRecords.put(record.getLsn(), record);
        }
      }

      executorService.shutdown();
      Assert.assertTrue(executorService.awaitTermination(15, TimeUnit.MINUTES));

      System.out.println("Assert WAL content 1");
      assertMTWALInsertion(wal, addedRecords);

      Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

      OLogSequenceNumber lastLSN = addedRecords.lastKey();
      Assert.assertEquals(wal.end(), lastLSN);

      wal.close();

      OCASDiskWriteAheadLog loadedWAL = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64,
          Integer.MAX_VALUE, 10 * 1024 * 1024, 20, true, Locale.US, 3 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

      System.out.println("Assert WAL content 2");
      assertMTWALInsertion(loadedWAL, addedRecords);

      Assert.assertEquals(loadedWAL.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(loadedWAL.end(), new OLogSequenceNumber(lastLSN.getSegment() + 1, OCASWALPage.RECORDS_OFFSET));

      loadedWAL.close();

      System.out.printf("%d iteration out of %d is passed\n", n, iterations);
    }
  }

  @Test
  @Ignore
  public void appendMT10MSegSmallCacheBackwardTest() throws Exception {
    final int iterations = 240;
    for (int n = 0; n < iterations; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE,
          10 * 1024 * 1024, 20, true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

      Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

      AtomicBoolean walIsFull = new AtomicBoolean();

      final OCheckpointRequestListener checkpointRequestListener = () -> walIsFull.set(true);
      wal.addFullCheckpointListener(checkpointRequestListener);
      ExecutorService executorService = Executors.newCachedThreadPool();

      AtomicReference<Future<Void>> segmentAppender = new AtomicReference<>();

      final OSegmentOverflowListener listener = (segment) -> {
        Future<Void> oldAppender = segmentAppender.get();

        while (oldAppender == null || oldAppender.isDone()) {
          if (wal.activeSegment() <= segment) {
            final Future<Void> appender = executorService.submit(new SegmentAdder(segment, wal));

            if (segmentAppender.compareAndSet(oldAppender, appender)) {
              break;
            }

            appender.cancel(false);
            oldAppender = segmentAppender.get();
          } else {
            break;
          }
        }
      };

      wal.addSegmentOverflowListener(listener);

      final TreeMap<OLogSequenceNumber, TestRecord> addedRecords = new TreeMap<>();
      final List<Future<List<TestRecord>>> futures = new ArrayList<>();

      for (int i = 0; i < 8; i++) {
        futures.add(executorService.submit(new RecordsAdderBackwardIteration(wal, walIsFull)));
      }

      for (Future<List<TestRecord>> future : futures) {
        final List<TestRecord> records = future.get();
        for (TestRecord record : records) {
          addedRecords.put(record.getLsn(), record);
        }
      }

      executorService.shutdown();
      Assert.assertTrue(executorService.awaitTermination(15, TimeUnit.MINUTES));

      System.out.println("Assert WAL content 1");
      assertMTWALInsertion(wal, addedRecords);

      Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(wal.end(), addedRecords.lastKey());

      wal.close();

      OCASDiskWriteAheadLog loadedWAL = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64,
          Integer.MAX_VALUE, 10 * 1024 * 1024, 20, true, Locale.US, 3 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

      System.out.println("Assert WAL content 2");
      assertMTWALInsertion(loadedWAL, addedRecords);

      Assert.assertEquals(loadedWAL.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(loadedWAL.end(),
          new OLogSequenceNumber(addedRecords.lastKey().getSegment() + 1, OCASWALPage.RECORDS_OFFSET));

      loadedWAL.close();

      System.out.printf("%d iteration out of %d is passed\n", n, iterations);
    }
  }

  @Test
  @Ignore
  public void appendMT10MSegBigCacheTest() throws Exception {
    final int iterations = 240;
    for (int n = 0; n < iterations; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64, Integer.MAX_VALUE,
          10 * 1024 * 1024, 20, true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

      Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

      AtomicBoolean walIsFull = new AtomicBoolean();

      final OCheckpointRequestListener checkpointRequestListener = () -> walIsFull.set(true);
      wal.addFullCheckpointListener(checkpointRequestListener);
      ExecutorService executorService = Executors.newCachedThreadPool();

      AtomicReference<Future<Void>> segmentAppender = new AtomicReference<>();

      final OSegmentOverflowListener listener = (segment) -> {
        Future<Void> oldAppender = segmentAppender.get();

        while (oldAppender == null || oldAppender.isDone()) {
          if (wal.activeSegment() <= segment) {
            final Future<Void> appender = executorService.submit(new SegmentAdder(segment, wal));

            if (segmentAppender.compareAndSet(oldAppender, appender)) {
              break;
            }

            appender.cancel(false);
            oldAppender = segmentAppender.get();
          } else {
            break;
          }
        }
      };

      wal.addSegmentOverflowListener(listener);

      final TreeMap<OLogSequenceNumber, TestRecord> addedRecords = new TreeMap<>();
      final List<Future<List<TestRecord>>> futures = new ArrayList<>();

      for (int i = 0; i < 8; i++) {
        futures.add(executorService.submit(new RecordsAdder(wal, walIsFull)));
      }

      for (Future<List<TestRecord>> future : futures) {
        final List<TestRecord> records = future.get();
        for (TestRecord record : records) {
          addedRecords.put(record.getLsn(), record);
        }
      }

      executorService.shutdown();
      Assert.assertTrue(executorService.awaitTermination(15, TimeUnit.MINUTES));

      System.out.println("Assert WAL content 1");
      assertMTWALInsertion(wal, addedRecords);

      Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(wal.end(), addedRecords.lastKey());
      wal.close();

      OCASDiskWriteAheadLog loadedWAL = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64,
          Integer.MAX_VALUE, 10 * 1024 * 1024, 20, true, Locale.US, 3 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

      System.out.println("Assert WAL content 2");
      assertMTWALInsertion(loadedWAL, addedRecords);

      Assert.assertEquals(loadedWAL.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(loadedWAL.end(),
          new OLogSequenceNumber(addedRecords.lastKey().getSegment() + 1, OCASWALPage.RECORDS_OFFSET));

      loadedWAL.close();

      System.out.printf("%d iteration out of %d is passed\n", n, iterations);
    }
  }

  @Test
  @Ignore
  public void appendMT10MSegBigCacheBackwardTest() throws Exception {
    final int iterations = 240;
    for (int n = 0; n < iterations; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64, Integer.MAX_VALUE,
          10 * 1024 * 1024, 20, true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

      Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

      AtomicBoolean walIsFull = new AtomicBoolean();

      final OCheckpointRequestListener checkpointRequestListener = () -> walIsFull.set(true);
      wal.addFullCheckpointListener(checkpointRequestListener);
      ExecutorService executorService = Executors.newCachedThreadPool();

      AtomicReference<Future<Void>> segmentAppender = new AtomicReference<>();

      final OSegmentOverflowListener listener = (segment) -> {
        Future<Void> oldAppender = segmentAppender.get();

        while (oldAppender == null || oldAppender.isDone()) {
          if (wal.activeSegment() <= segment) {
            final Future<Void> appender = executorService.submit(new SegmentAdder(segment, wal));

            if (segmentAppender.compareAndSet(oldAppender, appender)) {
              break;
            }

            appender.cancel(false);
            oldAppender = segmentAppender.get();
          } else {
            break;
          }
        }
      };

      wal.addSegmentOverflowListener(listener);

      final TreeMap<OLogSequenceNumber, TestRecord> addedRecords = new TreeMap<>();
      final List<Future<List<TestRecord>>> futures = new ArrayList<>();

      for (int i = 0; i < 8; i++) {
        futures.add(executorService.submit(new RecordsAdderBackwardIteration(wal, walIsFull)));
      }

      for (Future<List<TestRecord>> future : futures) {
        final List<TestRecord> records = future.get();
        for (TestRecord record : records) {
          addedRecords.put(record.getLsn(), record);
        }
      }

      executorService.shutdown();
      Assert.assertTrue(executorService.awaitTermination(15, TimeUnit.MINUTES));

      System.out.println("Assert WAL content 1");
      assertMTWALInsertion(wal, addedRecords);

      Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(wal.end(), addedRecords.lastKey());

      wal.close();

      OCASDiskWriteAheadLog loadedWAL = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64,
          Integer.MAX_VALUE, 10 * 1024 * 1024, 20, true, Locale.US, 3 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

      System.out.println("Assert WAL content 2");
      assertMTWALInsertion(loadedWAL, addedRecords);

      Assert.assertEquals(loadedWAL.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(loadedWAL.end(),
          new OLogSequenceNumber(addedRecords.lastKey().getSegment() + 1, OCASWALPage.RECORDS_OFFSET));

      loadedWAL.close();

      System.out.printf("%d iteration out of %d is passed\n", n, iterations);
    }
  }

  @Test
  @Ignore
  public void appendMT256MSegSmallCacheTest() throws Exception {
    final int iterations = 240;
    for (int n = 0; n < iterations; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE,
          256 * 1024 * 1024, 20, true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

      Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

      AtomicBoolean walIsFull = new AtomicBoolean();

      final OCheckpointRequestListener checkpointRequestListener = () -> walIsFull.set(true);
      wal.addFullCheckpointListener(checkpointRequestListener);
      ExecutorService executorService = Executors.newCachedThreadPool();

      AtomicReference<Future<Void>> segmentAppender = new AtomicReference<>();

      final OSegmentOverflowListener listener = (segment) -> {
        Future<Void> oldAppender = segmentAppender.get();

        while (oldAppender == null || oldAppender.isDone()) {
          if (wal.activeSegment() <= segment) {
            final Future<Void> appender = executorService.submit(new SegmentAdder(segment, wal));

            if (segmentAppender.compareAndSet(oldAppender, appender)) {
              break;
            }

            appender.cancel(false);
            oldAppender = segmentAppender.get();
          } else {
            break;
          }
        }
      };

      wal.addSegmentOverflowListener(listener);

      final TreeMap<OLogSequenceNumber, TestRecord> addedRecords = new TreeMap<>();
      final List<Future<List<TestRecord>>> futures = new ArrayList<>();

      for (int i = 0; i < 8; i++) {
        futures.add(executorService.submit(new RecordsAdder(wal, walIsFull)));
      }

      for (Future<List<TestRecord>> future : futures) {
        final List<TestRecord> records = future.get();
        for (TestRecord record : records) {
          addedRecords.put(record.getLsn(), record);
        }
      }

      executorService.shutdown();
      Assert.assertTrue(executorService.awaitTermination(15, TimeUnit.MINUTES));

      System.out.println("Assert WAL content 1");
      assertMTWALInsertion(wal, addedRecords);

      Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(wal.end(), addedRecords.lastKey());

      wal.close();

      OCASDiskWriteAheadLog loadedWAL = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64,
          Integer.MAX_VALUE, 10 * 1024 * 1024, 20, true, Locale.US, 3 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

      System.out.println("Assert WAL content 2");
      assertMTWALInsertion(loadedWAL, addedRecords);

      Assert.assertEquals(loadedWAL.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(loadedWAL.end(),
          new OLogSequenceNumber(addedRecords.lastKey().getSegment() + 1, OCASWALPage.RECORDS_OFFSET));

      loadedWAL.close();

      System.out.printf("%d iteration out of %d is passed\n", n, iterations);
    }
  }

  @Test
  @Ignore
  public void appendMT256MSegSmallCacheBackwardTest() throws Exception {
    final int iterations = 240;
    for (int n = 0; n < iterations; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE,
          256 * 1024 * 1024, 20, true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

      Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

      AtomicBoolean walIsFull = new AtomicBoolean();

      final OCheckpointRequestListener checkpointRequestListener = () -> walIsFull.set(true);
      wal.addFullCheckpointListener(checkpointRequestListener);
      ExecutorService executorService = Executors.newCachedThreadPool();

      AtomicReference<Future<Void>> segmentAppender = new AtomicReference<>();

      final OSegmentOverflowListener listener = (segment) -> {
        Future<Void> oldAppender = segmentAppender.get();

        while (oldAppender == null || oldAppender.isDone()) {
          if (wal.activeSegment() <= segment) {
            final Future<Void> appender = executorService.submit(new SegmentAdder(segment, wal));

            if (segmentAppender.compareAndSet(oldAppender, appender)) {
              break;
            }

            appender.cancel(false);
            oldAppender = segmentAppender.get();
          } else {
            break;
          }
        }
      };

      wal.addSegmentOverflowListener(listener);

      final TreeMap<OLogSequenceNumber, TestRecord> addedRecords = new TreeMap<>();
      final List<Future<List<TestRecord>>> futures = new ArrayList<>();

      for (int i = 0; i < 8; i++) {
        futures.add(executorService.submit(new RecordsAdderBackwardIteration(wal, walIsFull)));
      }

      for (Future<List<TestRecord>> future : futures) {
        final List<TestRecord> records = future.get();
        for (TestRecord record : records) {
          addedRecords.put(record.getLsn(), record);
        }
      }

      executorService.shutdown();
      Assert.assertTrue(executorService.awaitTermination(15, TimeUnit.MINUTES));

      System.out.println("Assert WAL content 1");
      assertMTWALInsertion(wal, addedRecords);

      Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(wal.end(), addedRecords.lastKey());

      wal.close();

      OCASDiskWriteAheadLog loadedWAL = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64,
          Integer.MAX_VALUE, 10 * 1024 * 1024, 20, true, Locale.US, 3 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

      System.out.println("Assert WAL content 2");
      assertMTWALInsertion(loadedWAL, addedRecords);

      Assert.assertEquals(loadedWAL.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(loadedWAL.end(),
          new OLogSequenceNumber(addedRecords.lastKey().getSegment() + 1, OCASWALPage.RECORDS_OFFSET));

      loadedWAL.close();

      System.out.printf("%d iteration out of %d is passed\n", n, iterations);
    }
  }

  @Test
  @Ignore
  public void appendMT256MSegBigCacheTest() throws Exception {
    final int iterations = 240;
    for (int n = 0; n < iterations; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64, Integer.MAX_VALUE,
          256 * 1024 * 1024, 20, true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

      Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

      AtomicBoolean walIsFull = new AtomicBoolean();

      final OCheckpointRequestListener checkpointRequestListener = () -> walIsFull.set(true);
      wal.addFullCheckpointListener(checkpointRequestListener);
      ExecutorService executorService = Executors.newCachedThreadPool();

      AtomicReference<Future<Void>> segmentAppender = new AtomicReference<>();

      final OSegmentOverflowListener listener = (segment) -> {
        Future<Void> oldAppender = segmentAppender.get();

        while (oldAppender == null || oldAppender.isDone()) {
          if (wal.activeSegment() <= segment) {
            final Future<Void> appender = executorService.submit(new SegmentAdder(segment, wal));

            if (segmentAppender.compareAndSet(oldAppender, appender)) {
              break;
            }

            appender.cancel(false);
            oldAppender = segmentAppender.get();
          } else {
            break;
          }
        }
      };

      wal.addSegmentOverflowListener(listener);

      final TreeMap<OLogSequenceNumber, TestRecord> addedRecords = new TreeMap<>();
      final List<Future<List<TestRecord>>> futures = new ArrayList<>();

      for (int i = 0; i < 8; i++) {
        futures.add(executorService.submit(new RecordsAdder(wal, walIsFull)));
      }

      for (Future<List<TestRecord>> future : futures) {
        final List<TestRecord> records = future.get();
        for (TestRecord record : records) {
          addedRecords.put(record.getLsn(), record);
        }
      }

      executorService.shutdown();
      Assert.assertTrue(executorService.awaitTermination(15, TimeUnit.MINUTES));

      System.out.println("Assert WAL content 1");
      assertMTWALInsertion(wal, addedRecords);

      Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(wal.end(), addedRecords.lastKey());

      wal.close();

      OCASDiskWriteAheadLog loadedWAL = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64,
          Integer.MAX_VALUE, 10 * 1024 * 1024, 20, true, Locale.US, 3 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

      System.out.println("Assert WAL content 2");
      assertMTWALInsertion(loadedWAL, addedRecords);

      Assert.assertEquals(loadedWAL.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(loadedWAL.end(),
          new OLogSequenceNumber(addedRecords.lastKey().getSegment() + 1, OCASWALPage.RECORDS_OFFSET));

      loadedWAL.close();

      System.out.printf("%d iteration out of %d is passed\n", n, iterations);
    }
  }

  @Test
  @Ignore
  public void appendMT256MSegBigCacheBackwardTest() throws Exception {
    final int iterations = 240;
    for (int n = 0; n < iterations; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64, Integer.MAX_VALUE,
          256 * 1024 * 1024, 20, true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

      Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

      AtomicBoolean walIsFull = new AtomicBoolean();

      final OCheckpointRequestListener checkpointRequestListener = () -> walIsFull.set(true);
      wal.addFullCheckpointListener(checkpointRequestListener);
      ExecutorService executorService = Executors.newCachedThreadPool();

      AtomicReference<Future<Void>> segmentAppender = new AtomicReference<>();

      final OSegmentOverflowListener listener = (segment) -> {
        Future<Void> oldAppender = segmentAppender.get();

        while (oldAppender == null || oldAppender.isDone()) {
          if (wal.activeSegment() <= segment) {
            final Future<Void> appender = executorService.submit(new SegmentAdder(segment, wal));

            if (segmentAppender.compareAndSet(oldAppender, appender)) {
              break;
            }

            appender.cancel(false);
            oldAppender = segmentAppender.get();
          } else {
            break;
          }
        }
      };

      wal.addSegmentOverflowListener(listener);

      final TreeMap<OLogSequenceNumber, TestRecord> addedRecords = new TreeMap<>();
      final List<Future<List<TestRecord>>> futures = new ArrayList<>();

      for (int i = 0; i < 8; i++) {
        futures.add(executorService.submit(new RecordsAdderBackwardIteration(wal, walIsFull)));
      }

      for (Future<List<TestRecord>> future : futures) {
        final List<TestRecord> records = future.get();
        for (TestRecord record : records) {
          addedRecords.put(record.getLsn(), record);
        }
      }

      executorService.shutdown();
      Assert.assertTrue(executorService.awaitTermination(15, TimeUnit.MINUTES));

      System.out.println("Assert WAL content 1");
      assertMTWALInsertion(wal, addedRecords);

      Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(wal.end(), addedRecords.lastKey());

      wal.close();

      OCASDiskWriteAheadLog loadedWAL = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64,
          Integer.MAX_VALUE, 10 * 1024 * 1024, 20, true, Locale.US, 3 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

      System.out.println("Assert WAL content 2");
      assertMTWALInsertion(loadedWAL, addedRecords);

      Assert.assertEquals(loadedWAL.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(loadedWAL.end(),
          new OLogSequenceNumber(addedRecords.lastKey().getSegment() + 1, OCASWALPage.RECORDS_OFFSET));

      loadedWAL.close();

      System.out.printf("%d iteration out of %d is passed\n", n, iterations);
    }
  }

  @Test
  public void testMasterRecordOne() throws Exception {
    final int iterations = 1;

    for (int n = 0; n < iterations; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64, Integer.MAX_VALUE,
          256 * 1024 * 1024, 20, true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

      ThreadLocalRandom random = ThreadLocalRandom.current();

      int recordsCount = 1000;
      for (int i = 0; i < recordsCount; i++) {
        final TestRecord walRecord = new TestRecord(random, 2 * wal.pageSize(), 1);

        OLogSequenceNumber lsn = wal.log(walRecord);
        Assert.assertEquals(walRecord.getLsn(), lsn);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), lsn);
      }

      OLogSequenceNumber masterLSN = wal.log(new OFullCheckpointStartRecord());
      OLogSequenceNumber checkpointLSN = wal.lastCheckpoint();

      while (checkpointLSN == null) {
        checkpointLSN = wal.lastCheckpoint();
      }

      Assert.assertEquals(masterLSN, checkpointLSN);

      wal.close();

      OCASDiskWriteAheadLog loadedWAL = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64,
          Integer.MAX_VALUE, 256 * 1024 * 1024, 20, true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

      Assert.assertEquals(masterLSN, loadedWAL.lastCheckpoint());

      loadedWAL.close();
    }
  }

  @Test
  public void testMasterSeveralMasterRecords() throws Exception {
    for (int n = 0; n < 1; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64, Integer.MAX_VALUE,
          256 * 1024 * 1024, 20, true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

      long seed = System.nanoTime();
      Random random = new Random(seed);

      TreeSet<OLogSequenceNumber> masterRecords = new TreeSet<>();

      OLogSequenceNumber lastCheckpoint = null;

      for (int k = 0; k < 1000; k++) {
        int recordsCount = 20;
        for (int i = 0; i < recordsCount; i++) {
          final TestRecord walRecord = new TestRecord(random, 2 * wal.pageSize(), 1);

          OLogSequenceNumber lsn = wal.log(walRecord);
          Assert.assertEquals(walRecord.getLsn(), lsn);

          Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
          Assert.assertEquals(wal.end(), lsn);
        }

        final OLogSequenceNumber masterLSN = wal.log(new OFullCheckpointStartRecord());
        masterRecords.add(masterLSN);

        OLogSequenceNumber checkpoint = wal.lastCheckpoint();
        if (lastCheckpoint != null) {
          Assert.assertTrue(checkpoint.compareTo(lastCheckpoint) >= 0);
        }
        lastCheckpoint = checkpoint;

        if (checkpoint != null) {
          Assert.assertTrue(masterRecords.contains(checkpoint));
        }
      }

      //noinspection StatementWithEmptyBody
      while (wal.getFlushedLsn() == null || wal.end().compareTo(wal.getFlushedLsn()) > 0)
        ;

      Assert.assertEquals(wal.end(), wal.getFlushedLsn());
      Assert.assertEquals(masterRecords.last(), wal.lastCheckpoint());

      wal.close();

      OCASDiskWriteAheadLog loadedWAL = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64,
          Integer.MAX_VALUE, 256 * 1024 * 1024, 20, true, Locale.US, 10 * 1024 * 1024 * 1024L,

          -1, 1000, true, false, true, 10);
      Assert.assertEquals(masterRecords.last(), wal.lastCheckpoint());

      loadedWAL.close();
    }
  }

  @Test
  public void testFlush() throws Exception {
    final int iterations = 1;

    for (int n = 0; n < iterations; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64, Integer.MAX_VALUE,
          256 * 1024 * 1024, 20, true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

      long seed = System.nanoTime();
      Random random = new Random(seed);

      OLogSequenceNumber lastLSN = null;
      for (int k = 0; k < 10000; k++) {
        int recordsCount = 20;
        for (int i = 0; i < recordsCount; i++) {
          final TestRecord walRecord = new TestRecord(random, 2 * wal.pageSize(), 1);

          OLogSequenceNumber lsn = wal.log(walRecord);
          Assert.assertEquals(walRecord.getLsn(), lsn);

          Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
          Assert.assertEquals(wal.end(), lsn);
          lastLSN = lsn;
        }

        wal.flush();

        Assert.assertEquals(lastLSN, wal.getFlushedLsn());
      }

      wal.close();

      OCASDiskWriteAheadLog loadedWAL = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64,
          Integer.MAX_VALUE, 256 * 1024 * 1024, 20, true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

      Assert.assertNotNull(loadedWAL.getFlushedLsn());
      Assert.assertEquals(loadedWAL.end(), loadedWAL.getFlushedLsn());

      loadedWAL.close();

      System.out.printf("%d iterations out of %d is passed \n", n, iterations);
    }
  }

  @Test
  public void cutTillTest() throws Exception {
    int iterations = 1;
    for (int n = 0; n < iterations; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      final long seed = System.nanoTime();
      final Random random = new Random(seed);

      TreeSet<Long> segments = new TreeSet<>();
      TreeMap<OLogSequenceNumber, TestRecord> records = new TreeMap<>();

      OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64, Integer.MAX_VALUE,
          10 * 1024 * 1024, 20, true, Locale.US, 10 * 1024 * 1024L, -1, 1000, true, false, true, 10);

      ExecutorService executorService = Executors.newCachedThreadPool();
      AtomicReference<Future<Void>> segmentAppender = new AtomicReference<>();

      final OSegmentOverflowListener listener = (segment) -> {
        Future<Void> oldAppender = segmentAppender.get();

        while (oldAppender == null || oldAppender.isDone()) {
          if (wal.activeSegment() <= segment) {
            final Future<Void> appender = executorService.submit(new SegmentAdder(segment, wal));

            if (segmentAppender.compareAndSet(oldAppender, appender)) {
              break;
            }

            appender.cancel(false);
            oldAppender = segmentAppender.get();
          } else {
            break;
          }
        }
      };

      wal.addSegmentOverflowListener(listener);

      OLogSequenceNumber begin = null;
      OLogSequenceNumber end = null;

      for (int k = 0; k < 10; k++) {
        for (int i = 0; i < 30_000; i++) {
          final TestRecord walRecord = new TestRecord(random, 4 * wal.pageSize(), 2 * wal.pageSize());
          OLogSequenceNumber lsn = wal.log(walRecord);
          records.put(lsn, walRecord);

          segments.add(lsn.getSegment());
        }

        long minSegment = segments.first();
        long maxSegment = segments.last();

        long segment = random.nextInt((int) (maxSegment - minSegment)) + minSegment;
        final long[] notActive = wal.nonActiveSegments();

        wal.cutAllSegmentsSmallerThan(segment);

        begin = wal.begin();
        final int cutSegmentIndex = Arrays.binarySearch(notActive, segment);

        if (cutSegmentIndex >= 0) {
          Assert.assertTrue(begin.getSegment() >= notActive[cutSegmentIndex]);
        } else {
          Assert.assertTrue(begin.getSegment() > notActive[notActive.length - 1]);
        }

        begin = wal.begin();
        end = wal.end();

        segments.headSet(segment, false).clear();
        for (TestRecord record : records.values()) {
          if (record.getLsn().getSegment() < begin.getSegment()) {
            Assert.assertTrue(wal.read(record.getLsn(), 1).isEmpty());
          } else {
            Assert.assertArrayEquals(record.data, ((TestRecord) (wal.read(record.getLsn(), 1).get(0))).data);
          }
        }

        records.headMap(begin, false).clear();

        for (int i = 0; i < begin.getSegment(); i++) {
          final Path segmentPath = testDirectory.resolve(getSegmentName(i));
          Assert.assertTrue(!Files.exists(segmentPath));
        }

        {
          final Path segmentPath = testDirectory.resolve(getSegmentName(end.getSegment() + 1));
          Assert.assertTrue(!Files.exists(segmentPath));
        }
      }

      wal.close();

      OCASDiskWriteAheadLog loadedWAL = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64,
          Integer.MAX_VALUE, 10 * 1024 * 1024, 20, true, Locale.US, 10 * 1024 * 1024L, -1, 1000, true, false, true, 10);

      long minSegment = begin.getSegment();
      long maxSegment = end.getSegment();

      long segment = random.nextInt((int) (maxSegment - minSegment)) + minSegment;
      loadedWAL.cutAllSegmentsSmallerThan(segment);

      Assert.assertEquals(new OLogSequenceNumber(segment, OCASWALPage.RECORDS_OFFSET), loadedWAL.begin());
      Assert.assertEquals(new OLogSequenceNumber(end.getSegment() + 1, OCASWALPage.RECORDS_OFFSET), loadedWAL.end());

      for (TestRecord record : records.values()) {
        if (record.getLsn().getSegment() < segment) {
          Assert.assertTrue(loadedWAL.read(record.getLsn(), 1).isEmpty());
        } else {
          Assert.assertArrayEquals(record.data, ((TestRecord) (loadedWAL.read(record.getLsn(), 1).get(0))).data);
        }
      }

      begin = loadedWAL.begin();
      end = loadedWAL.end();

      for (int i = 0; i < begin.getSegment(); i++) {
        final Path segmentPath = testDirectory.resolve(getSegmentName(i));
        Assert.assertTrue(!Files.exists(segmentPath));
      }

      {
        final Path segmentPath = testDirectory.resolve(getSegmentName(end.getSegment() + 1));
        Assert.assertTrue(!Files.exists(segmentPath));
      }

      loadedWAL.close();

      System.out.printf("%d iterations out of %d are passed \n", n, iterations);
    }
  }

  @Test
  public void testCutTillLimit() throws Exception {
    OFileUtils.deleteRecursively(testDirectory.toFile());

    final long seed = System.nanoTime();
    final Random random = new Random(seed);

    final TreeMap<OLogSequenceNumber, TestRecord> records = new TreeMap<>();

    OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64, Integer.MAX_VALUE,
        10 * 1024 * 1024, 20, true, Locale.US, 10 * 1024 * 1024L, -1, 1000, true, false, true, 10);

    AtomicReference<Future<Void>> segmentAppender = new AtomicReference<>();
    ExecutorService executorService = Executors.newCachedThreadPool();

    final OSegmentOverflowListener listener = (segment) -> {
      Future<Void> oldAppender = segmentAppender.get();

      while (oldAppender == null || oldAppender.isDone()) {
        if (wal.activeSegment() <= segment) {
          final Future<Void> appender = executorService.submit(new SegmentAdder(segment, wal));

          if (segmentAppender.compareAndSet(oldAppender, appender)) {
            break;
          }

          appender.cancel(false);
          oldAppender = segmentAppender.get();
        } else {
          break;
        }
      }
    };

    wal.addSegmentOverflowListener(listener);

    for (int k = 0; k < 10; k++) {
      for (int i = 0; i < 30_000; i++) {
        final TestRecord walRecord = new TestRecord(random, 4 * wal.pageSize(), 2 * wal.pageSize());
        OLogSequenceNumber lsn = wal.log(walRecord);
        records.put(lsn, walRecord);
      }

      final TreeMap<OLogSequenceNumber, Integer> limits = new TreeMap<>();

      OLogSequenceNumber lsn = chooseRandomRecord(random, records);
      addLimit(limits, lsn);
      wal.addCutTillLimit(lsn);

      lsn = chooseRandomRecord(random, records);
      addLimit(limits, lsn);
      wal.addCutTillLimit(lsn);

      lsn = chooseRandomRecord(random, records);
      addLimit(limits, lsn);
      wal.addCutTillLimit(lsn);

      long[] nonActive = wal.nonActiveSegments();
      wal.cutTill(limits.lastKey());

      long segment = limits.firstKey().getSegment();
      OLogSequenceNumber begin = wal.begin();
      checkThatAllNonActiveSegmentsAreRemoved(nonActive, segment, wal);

      Assert.assertTrue(begin.getSegment() <= segment);

      lsn = limits.firstKey();
      removeLimit(limits, lsn);
      wal.removeCutTillLimit(lsn);

      nonActive = wal.nonActiveSegments();
      wal.cutTill(limits.lastKey());

      segment = limits.firstKey().getSegment();
      begin = wal.begin();

      checkThatAllNonActiveSegmentsAreRemoved(nonActive, segment, wal);
      checkThatSegmentsBellowAreRemoved(wal);

      Assert.assertTrue(begin.getSegment() <= segment);

      lsn = limits.lastKey();
      removeLimit(limits, lsn);
      wal.removeCutTillLimit(lsn);

      nonActive = wal.nonActiveSegments();
      wal.cutTill(lsn);

      segment = limits.firstKey().getSegment();
      begin = wal.begin();

      checkThatAllNonActiveSegmentsAreRemoved(nonActive, segment, wal);
      checkThatSegmentsBellowAreRemoved(wal);
      Assert.assertTrue(begin.getSegment() <= segment);

      lsn = limits.lastKey();
      removeLimit(limits, lsn);
      wal.removeCutTillLimit(lsn);

      nonActive = wal.nonActiveSegments();
      wal.cutTill(lsn);
      checkThatAllNonActiveSegmentsAreRemoved(nonActive, lsn.getSegment(), wal);
      checkThatSegmentsBellowAreRemoved(wal);

      records.headMap(wal.begin(), false).clear();
    }

    wal.close();
  }

  @Test
  @Ignore
  public void testCutTillMT() throws Exception {
    OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64, Integer.MAX_VALUE,
        10 * 1024 * 1024, 20, true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

    AtomicReference<Future<Void>> segmentAppender = new AtomicReference<>();
    ExecutorService executorService = Executors.newCachedThreadPool();

    final OSegmentOverflowListener listener = (segment) -> {
      Future<Void> oldAppender = segmentAppender.get();

      while (oldAppender == null || oldAppender.isDone()) {
        if (wal.activeSegment() <= segment) {
          final Future<Void> appender = executorService.submit(new SegmentAdder(segment, wal));

          if (segmentAppender.compareAndSet(oldAppender, appender)) {
            break;
          }

          appender.cancel(false);
          oldAppender = segmentAppender.get();
        } else {
          break;
        }
      }
    };

    wal.addSegmentOverflowListener(listener);

    AtomicBoolean walIsFull = new AtomicBoolean();

    final OCheckpointRequestListener checkpointRequestListener = () -> walIsFull.set(true);
    wal.addFullCheckpointListener(checkpointRequestListener);

    final List<Future<TreeMap<OLogSequenceNumber, OWriteableWALRecord>>> futures = new ArrayList<>();
    final AtomicBoolean[] sendReport = new AtomicBoolean[8];

    for (int i = 0; i < 8; i++) {
      sendReport[i] = new AtomicBoolean();
      futures.add(executorService.submit(new CutTillTester(wal, walIsFull, sendReport[i])));
    }

    Timer timer = new Timer();
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        walIsFull.set(true);
      }
    }, 1000 * 60 * 60 * 10);

    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        for (AtomicBoolean report : sendReport) {
          report.set(true);
        }
      }
    }, 1000 * 30, 1000 * 30);

    TreeMap<OLogSequenceNumber, OWriteableWALRecord> addedRecords = new TreeMap<>();
    for (Future<TreeMap<OLogSequenceNumber, OWriteableWALRecord>> future : futures) {
      TreeMap<OLogSequenceNumber, OWriteableWALRecord> result = future.get();
      addedRecords.putAll(result);
    }

    System.out.println("Check added records");
    addedRecords.headMap(wal.begin(), false).clear();

    assertMTWALInsertion(wal, addedRecords);
    wal.close();
  }

  @Test
  public void testAppendSegment() throws Exception {
    int iterations = 1;
    for (int n = 0; n < iterations; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      final long seed = System.nanoTime();
      final Random random = new Random(seed);

      try {
        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64,
            Integer.MAX_VALUE, 10 * 1024 * 1024, 20, true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

        List<TestRecord> records = new ArrayList<>();

        System.out.println("Load data");
        final int recordsCount = 100_000;
        for (int i = 0; i < recordsCount; i++) {
          final TestRecord walRecord = new TestRecord(random, 3 * wal.pageSize(), 1);
          records.add(walRecord);

          OLogSequenceNumber lsn = wal.log(walRecord);
          Assert.assertEquals(walRecord.getLsn(), lsn);

          Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
          Assert.assertEquals(wal.end(), lsn);

          if (random.nextDouble() < 0.05) {
            final int segments = random.nextInt(5) + 1;

            for (int k = 0; k < segments; k++) {
              wal.appendNewSegment();
            }
          }
        }

        System.out.println("First check");
        for (int i = 0; i < recordsCount; i++) {
          final List<OWriteableWALRecord> result = wal.read(records.get(i).getLsn(), 500);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i, recordsCount).iterator();

          while (resultIterator.hasNext()) {
            TestRecord record = recordIterator.next();
            TestRecord resultRecord = (TestRecord) resultIterator.next();

            Assert.assertArrayEquals(record.data, resultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }
        }

        wal.close();

        System.out.println("Second check");
        wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE, Integer.MAX_VALUE, 20,
            true, Locale.US, -1, -1, 1000, true, false, true, 10);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(),
            new OLogSequenceNumber(records.get(records.size() - 1).getLsn().getSegment() + 1, OCASWALPage.RECORDS_OFFSET));

        for (int i = 0; i < recordsCount; i++) {
          final List<OWriteableWALRecord> result = wal.read(records.get(i).getLsn(), 500);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i, recordsCount).iterator();

          while (resultIterator.hasNext() && recordIterator.hasNext()) {
            TestRecord record = recordIterator.next();
            TestRecord resultRecord = (TestRecord) resultIterator.next();

            Assert.assertArrayEquals(record.data, resultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }
        }

        wal.close();

        Thread.sleep(2);
        System.out.printf("%d iterations out of %d were passed\n", n, iterations);
      } catch (Error | Exception e) {
        System.out.println("testAppendSegment seed : " + seed);
        throw e;
      }
    }
  }

  @Test
  public void testAppendSegmentNext() throws Exception {
    final int iterations = 1;
    for (int n = 0; n < iterations; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      final long seed = System.nanoTime();
      final Random random = new Random(seed);

      try {
        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64,
            Integer.MAX_VALUE, 10 * 1024 * 1024, 20, true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

        List<TestRecord> records = new ArrayList<>();

        System.out.println("Load data");
        final int recordsCount = 100_000;
        for (int i = 0; i < recordsCount; i++) {
          final TestRecord walRecord = new TestRecord(random, 3 * wal.pageSize(), 1);
          records.add(walRecord);

          OLogSequenceNumber lsn = wal.log(walRecord);
          Assert.assertEquals(walRecord.getLsn(), lsn);

          Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
          Assert.assertEquals(wal.end(), lsn);

          if (random.nextDouble() < 0.05) {
            final int segments = random.nextInt(5) + 1;

            for (int k = 0; k < segments; k++) {
              wal.appendNewSegment();
            }
          }
        }

        System.out.println("First check");
        for (int i = 0; i < recordsCount - 1; i++) {
          final List<OWriteableWALRecord> result = wal.next(records.get(i).getLsn(), 500);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i + 1, recordsCount).iterator();

          while (resultIterator.hasNext()) {
            TestRecord record = recordIterator.next();
            TestRecord resultRecord = (TestRecord) resultIterator.next();

            Assert.assertArrayEquals(record.data, resultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }
        }

        Assert.assertTrue(wal.next(records.get(recordsCount - 1).getLsn(), 500).isEmpty());

        wal.close();

        System.out.println("Second check");
        wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 64, Integer.MAX_VALUE, Integer.MAX_VALUE, 20,
            true, Locale.US, -1, -1, 1000, true, false, true, 10);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(),
            new OLogSequenceNumber(records.get(records.size() - 1).getLsn().getSegment() + 1, OCASWALPage.RECORDS_OFFSET));

        for (int i = 0; i < recordsCount - 1; i++) {
          final List<OWriteableWALRecord> result = wal.next(records.get(i).getLsn(), 500);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i + 1, recordsCount).iterator();

          while (resultIterator.hasNext() && recordIterator.hasNext()) {
            TestRecord record = recordIterator.next();
            TestRecord resultRecord = (TestRecord) resultIterator.next();

            Assert.assertArrayEquals(record.data, resultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }
        }

        List<OWriteableWALRecord> lastResult = wal.next(records.get(recordsCount - 1).getLsn(), 10);
        Assert.assertEquals(lastResult.size(), 1);
        OWriteableWALRecord emptyRecord = lastResult.get(0);

        Assert.assertTrue(emptyRecord instanceof OEmptyWALRecord);

        wal.close();

        Thread.sleep(2);
        System.out.printf("%d iterations out of %d were passed\n", n, iterations);
      } catch (Error | Exception e) {
        System.out.println("testAppendSegmentNext seed : " + seed);
        throw e;
      }
    }
  }

  @Test
  public void testDelete() throws Exception {
    OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64, Integer.MAX_VALUE,
        10 * 1024 * 1024, 20, true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

    final long seed = System.nanoTime();
    final Random random = new Random(seed);
    System.out.println("testDelete seed : " + seed);

    final int recordsCount = 30_000;
    for (int i = 0; i < recordsCount; i++) {
      final TestRecord walRecord = new TestRecord(random, 3 * wal.pageSize(), 1);

      OLogSequenceNumber lsn = wal.log(walRecord);
      Assert.assertEquals(walRecord.getLsn(), lsn);

      Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(wal.end(), lsn);

      if (random.nextDouble() < 0.05) {
        final int segments = random.nextInt(5) + 1;

        for (int k = 0; k < segments; k++) {
          wal.appendNewSegment();
        }
      }
    }

    wal.delete();

    Assert.assertTrue(Files.exists(testDirectory));
    Assert.assertEquals(0, testDirectory.toFile().listFiles().length);
  }

  @Test
  @Ignore
  public void testAddSmallRecords10MSeg() throws Exception {
    final int iterations = 1;
    for (int n = 0; n < iterations; n++) {
      final long seed = System.nanoTime();
      final Random random = new Random(seed);

      try {
        final long walSizeLimit = 2 * 1024 * 1024 * 1024L;
        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64,
            Integer.MAX_VALUE, 10 * 1024 * 1024, 20, true, Locale.US, walSizeLimit, -1, 1000, true, false, true, 10);

        AtomicBoolean walIsFull = new AtomicBoolean();

        final OCheckpointRequestListener checkpointRequestListener = () -> walIsFull.set(true);
        wal.addFullCheckpointListener(checkpointRequestListener);
        ExecutorService executorService = Executors.newCachedThreadPool();

        AtomicReference<Future<Void>> segmentAppender = new AtomicReference<>();

        final OSegmentOverflowListener listener = (segment) -> {
          Future<Void> oldAppender = segmentAppender.get();

          while (oldAppender == null || oldAppender.isDone()) {
            if (wal.activeSegment() <= segment) {
              final Future<Void> appender = executorService.submit(new SegmentAdder(segment, wal));

              if (segmentAppender.compareAndSet(oldAppender, appender)) {
                break;
              }

              appender.cancel(false);
              oldAppender = segmentAppender.get();
            } else {
              break;
            }
          }
        };

        wal.addSegmentOverflowListener(listener);

        boolean[] report = new boolean[1];
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
          @Override
          public void run() {
            report[0] = true;
          }
        }, 1000 * 30, 1000 * 30);

        System.out.println("Records load");
        List<TestRecord> records = new ArrayList<>();
        while (!walIsFull.get()) {
          final TestRecord walRecord = new TestRecord(random, 2, 1);
          records.add(walRecord);

          OLogSequenceNumber lsn = wal.log(walRecord);
          Assert.assertEquals(walRecord.getLsn(), lsn);

          Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
          Assert.assertEquals(wal.end(), lsn);

          if (report[0]) {
            System.out.printf("%d records were added, log is filled on %d%%\n", records.size(), 100L * wal.size() / walSizeLimit);
            report[0] = false;
          }
        }

        System.out.println("Records check");
        for (int i = 0; i < records.size(); i++) {
          final List<OWriteableWALRecord> result = wal.read(records.get(i).getLsn(), 10);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i, records.size()).iterator();

          while (resultIterator.hasNext()) {
            TestRecord record = recordIterator.next();
            TestRecord resultRecord = (TestRecord) resultIterator.next();

            Assert.assertArrayEquals(record.data, resultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }

          if (report[0]) {
            System.out.printf("%d records out of %d were checked\n", i, records.size());
            report[0] = false;
          }
        }

        wal.close();
        timer.cancel();
      } catch (Exception | Error e) {
        System.out.println("testAddSmallRecords10MSeg : " + seed);
        throw e;
      }
    }
  }

  @Test
  @Ignore
  public void testAddSmallRecords512MSeg() throws Exception {
    final int iterations = 1;
    for (int n = 0; n < iterations; n++) {
      final long seed = System.nanoTime();
      final Random random = new Random(seed);

      try {
        final long walSizeLimit = 2 * 1024 * 1024 * 1024L;
        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64,
            Integer.MAX_VALUE, 512 * 1024 * 1024, 20, true, Locale.US, walSizeLimit, -1, 1000, true, false, true, 10);

        AtomicBoolean walIsFull = new AtomicBoolean();

        final OCheckpointRequestListener checkpointRequestListener = () -> walIsFull.set(true);
        wal.addFullCheckpointListener(checkpointRequestListener);
        ExecutorService executorService = Executors.newCachedThreadPool();

        AtomicReference<Future<Void>> segmentAppender = new AtomicReference<>();

        final OSegmentOverflowListener listener = (segment) -> {
          Future<Void> oldAppender = segmentAppender.get();

          while (oldAppender == null || oldAppender.isDone()) {
            if (wal.activeSegment() <= segment) {
              final Future<Void> appender = executorService.submit(new SegmentAdder(segment, wal));

              if (segmentAppender.compareAndSet(oldAppender, appender)) {
                break;
              }

              appender.cancel(false);
              oldAppender = segmentAppender.get();
            } else {
              break;
            }
          }
        };

        wal.addSegmentOverflowListener(listener);

        boolean[] report = new boolean[1];
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
          @Override
          public void run() {
            report[0] = true;
          }
        }, 1000 * 30, 1000 * 30);

        System.out.println("Records load");
        List<TestRecord> records = new ArrayList<>();
        while (!walIsFull.get()) {
          final TestRecord walRecord = new TestRecord(random, 2, 1);
          records.add(walRecord);

          OLogSequenceNumber lsn = wal.log(walRecord);
          Assert.assertEquals(walRecord.getLsn(), lsn);

          Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
          Assert.assertEquals(wal.end(), lsn);

          if (report[0]) {
            System.out.printf("%d records were added, log is filled on %d%%\n", records.size(), 100L * wal.size() / walSizeLimit);
            report[0] = false;
          }
        }

        System.out.println("Records check");
        for (int i = 0; i < records.size(); i++) {
          final List<OWriteableWALRecord> result = wal.read(records.get(i).getLsn(), 10);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i, records.size()).iterator();

          while (resultIterator.hasNext()) {
            TestRecord record = recordIterator.next();
            TestRecord resultRecord = (TestRecord) resultIterator.next();

            Assert.assertArrayEquals(record.data, resultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }

          if (report[0]) {
            System.out.printf("%d records out of %d were checked\n", i, records.size());
            report[0] = false;
          }
        }

        wal.close();
        timer.cancel();
      } catch (Exception | Error e) {
        System.out.println("testAddSmallRecords512MSeg : " + seed);
        throw e;
      }
    }
  }

  @Test
  @Ignore
  public void testAddBigRecords10MSeg() throws Exception {
    final int iterations = 1;
    for (int n = 0; n < iterations; n++) {
      final long seed = System.nanoTime();
      final Random random = new Random(seed);

      try {
        final long walSizeLimit = 10 * 1024 * 1024 * 1024L;
        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64,
            Integer.MAX_VALUE, 10 * 1024 * 1024, 20, true, Locale.US, walSizeLimit, -1, 1000, true, false, true, 10);

        AtomicBoolean walIsFull = new AtomicBoolean();

        final OCheckpointRequestListener checkpointRequestListener = () -> walIsFull.set(true);
        wal.addFullCheckpointListener(checkpointRequestListener);
        ExecutorService executorService = Executors.newCachedThreadPool();

        AtomicReference<Future<Void>> segmentAppender = new AtomicReference<>();

        final OSegmentOverflowListener listener = (segment) -> {
          Future<Void> oldAppender = segmentAppender.get();

          while (oldAppender == null || oldAppender.isDone()) {
            if (wal.activeSegment() <= segment) {
              final Future<Void> appender = executorService.submit(new SegmentAdder(segment, wal));

              if (segmentAppender.compareAndSet(oldAppender, appender)) {
                break;
              }

              appender.cancel(false);
              oldAppender = segmentAppender.get();
            } else {
              break;
            }
          }
        };

        wal.addSegmentOverflowListener(listener);

        boolean[] report = new boolean[1];
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
          @Override
          public void run() {
            report[0] = true;
          }
        }, 1000 * 30, 1000 * 30);

        System.out.println("Records load");
        List<TestRecord> records = new ArrayList<>();
        while (!walIsFull.get()) {
          final TestRecord walRecord = new TestRecord(random, 4 * wal.pageSize(), 2 * wal.pageSize());
          records.add(walRecord);

          OLogSequenceNumber lsn = wal.log(walRecord);
          Assert.assertEquals(walRecord.getLsn(), lsn);

          Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
          Assert.assertEquals(wal.end(), lsn);

          if (report[0]) {
            System.out.printf("%d records were added, log is filled on %d%%\n", records.size(), 100L * wal.size() / walSizeLimit);
            report[0] = false;
          }
        }

        System.out.println("Records check");
        for (int i = 0; i < records.size(); i++) {
          final List<OWriteableWALRecord> result = wal.read(records.get(i).getLsn(), 10);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i, records.size()).iterator();

          while (resultIterator.hasNext()) {
            TestRecord record = recordIterator.next();
            TestRecord resultRecord = (TestRecord) resultIterator.next();

            Assert.assertArrayEquals(record.data, resultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }

          if (report[0]) {
            System.out.printf("%d records out of %d were checked\n", i, records.size());
            report[0] = false;
          }
        }

        wal.close();
        timer.cancel();
      } catch (Exception | Error e) {
        System.out.println("testAddBigRecords10MSeg : " + seed);
        throw e;
      }
    }
  }

  @Test
  @Ignore
  public void testAddBigRecords512MSeg() throws Exception {
    final int iterations = 1;
    for (int n = 0; n < iterations; n++) {
      final long seed = System.nanoTime();
      final Random random = new Random(seed);

      try {
        final long walSizeLimit = 10 * 1024 * 1024 * 1024L;
        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64,
            Integer.MAX_VALUE, 512 * 1024 * 1024, 20, true, Locale.US, walSizeLimit, -1, 1000, true, false, true, 10);

        AtomicBoolean walIsFull = new AtomicBoolean();

        final OCheckpointRequestListener checkpointRequestListener = () -> walIsFull.set(true);
        wal.addFullCheckpointListener(checkpointRequestListener);
        ExecutorService executorService = Executors.newCachedThreadPool();

        AtomicReference<Future<Void>> segmentAppender = new AtomicReference<>();

        final OSegmentOverflowListener listener = (segment) -> {
          Future<Void> oldAppender = segmentAppender.get();

          while (oldAppender == null || oldAppender.isDone()) {
            if (wal.activeSegment() <= segment) {
              final Future<Void> appender = executorService.submit(new SegmentAdder(segment, wal));

              if (segmentAppender.compareAndSet(oldAppender, appender)) {
                break;
              }

              appender.cancel(false);
              oldAppender = segmentAppender.get();
            } else {
              break;
            }
          }
        };

        wal.addSegmentOverflowListener(listener);

        boolean[] report = new boolean[1];
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
          @Override
          public void run() {
            report[0] = true;
          }
        }, 1000 * 30, 1000 * 30);

        System.out.println("Records load");
        List<TestRecord> records = new ArrayList<>();
        while (!walIsFull.get()) {
          final TestRecord walRecord = new TestRecord(random, 4 * wal.pageSize(), 2 * wal.pageSize());
          records.add(walRecord);

          OLogSequenceNumber lsn = wal.log(walRecord);
          Assert.assertEquals(walRecord.getLsn(), lsn);

          Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
          Assert.assertEquals(wal.end(), lsn);

          if (report[0]) {
            System.out.printf("%d records were added, log is filled on %d%%\n", records.size(), 100L * wal.size() / walSizeLimit);
            report[0] = false;
          }
        }

        System.out.println("Records check");
        for (int i = 0; i < records.size(); i++) {
          final List<OWriteableWALRecord> result = wal.read(records.get(i).getLsn(), 10);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i, records.size()).iterator();

          while (resultIterator.hasNext()) {
            TestRecord record = recordIterator.next();
            TestRecord resultRecord = (TestRecord) resultIterator.next();

            Assert.assertArrayEquals(record.data, resultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }

          if (report[0]) {
            System.out.printf("%d records out of %d were checked\n", i, records.size());
            report[0] = false;
          }
        }

        wal.close();
        timer.cancel();
      } catch (Exception | Error e) {
        System.out.println("testAddBigRecords512MSeg : " + seed);
        throw e;
      }
    }
  }

  @Test
  @Ignore
  public void testAddBigSmallRecords10MSeg() throws Exception {
    final int iterations = 1;
    for (int n = 0; n < iterations; n++) {
      final long seed = System.nanoTime();
      final Random random = new Random(seed);

      try {
        final long walSizeLimit = 5 * 1024 * 1024 * 1024L;
        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64,
            Integer.MAX_VALUE, 10 * 1024 * 1024, 20, true, Locale.US, walSizeLimit, -1, 1000, true, false, true, 10);

        AtomicBoolean walIsFull = new AtomicBoolean();

        final OCheckpointRequestListener checkpointRequestListener = () -> walIsFull.set(true);
        wal.addFullCheckpointListener(checkpointRequestListener);
        ExecutorService executorService = Executors.newCachedThreadPool();

        AtomicReference<Future<Void>> segmentAppender = new AtomicReference<>();

        final OSegmentOverflowListener listener = (segment) -> {
          Future<Void> oldAppender = segmentAppender.get();

          while (oldAppender == null || oldAppender.isDone()) {
            if (wal.activeSegment() <= segment) {
              final Future<Void> appender = executorService.submit(new SegmentAdder(segment, wal));

              if (segmentAppender.compareAndSet(oldAppender, appender)) {
                break;
              }

              appender.cancel(false);
              oldAppender = segmentAppender.get();
            } else {
              break;
            }
          }
        };

        wal.addSegmentOverflowListener(listener);

        boolean[] report = new boolean[1];
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
          @Override
          public void run() {
            report[0] = true;
          }
        }, 1000 * 30, 1000 * 30);

        System.out.println("Records load");
        List<TestRecord> records = new ArrayList<>();
        while (!walIsFull.get()) {
          final TestRecord walRecord;
          if (random.nextDouble() < 0.5) {
            walRecord = new TestRecord(random, 4 * wal.pageSize(), 2 * wal.pageSize());
          } else {
            walRecord = new TestRecord(random, 2, 1);
          }
          records.add(walRecord);

          OLogSequenceNumber lsn = wal.log(walRecord);
          Assert.assertEquals(walRecord.getLsn(), lsn);

          Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
          Assert.assertEquals(wal.end(), lsn);

          if (report[0]) {
            System.out.printf("%d records were added, log is filled on %d%%\n", records.size(), 100L * wal.size() / walSizeLimit);
            report[0] = false;
          }
        }

        System.out.println("Records check");
        for (int i = 0; i < records.size(); i++) {
          final List<OWriteableWALRecord> result = wal.read(records.get(i).getLsn(), 10);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i, records.size()).iterator();

          while (resultIterator.hasNext()) {
            TestRecord record = recordIterator.next();
            TestRecord resultRecord = (TestRecord) resultIterator.next();

            Assert.assertArrayEquals(record.data, resultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }

          if (report[0]) {
            System.out.printf("%d records out of %d were checked\n", i, records.size());
            report[0] = false;
          }
        }

        wal.close();
        timer.cancel();
      } catch (Exception | Error e) {
        System.out.println("testAddBigSmallRecords10MSeg : " + seed);
        throw e;
      }
    }
  }

  @Test
  @Ignore
  public void testAddBigSmallRecords512MSeg() throws Exception {
    final int iterations = 1;
    for (int n = 0; n < iterations; n++) {
      final long seed = System.nanoTime();
      final Random random = new Random(seed);

      try {
        final long walSizeLimit = 5 * 1024 * 1024 * 1024L;
        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64,
            Integer.MAX_VALUE, 512 * 1024 * 1024, 20, true, Locale.US, walSizeLimit, -1, 1000, true, false, true, 10);

        AtomicBoolean walIsFull = new AtomicBoolean();

        final OCheckpointRequestListener checkpointRequestListener = () -> walIsFull.set(true);
        wal.addFullCheckpointListener(checkpointRequestListener);
        ExecutorService executorService = Executors.newCachedThreadPool();

        AtomicReference<Future<Void>> segmentAppender = new AtomicReference<>();

        final OSegmentOverflowListener listener = (segment) -> {
          Future<Void> oldAppender = segmentAppender.get();

          while (oldAppender == null || oldAppender.isDone()) {
            if (wal.activeSegment() <= segment) {
              final Future<Void> appender = executorService.submit(new SegmentAdder(segment, wal));

              if (segmentAppender.compareAndSet(oldAppender, appender)) {
                break;
              }

              appender.cancel(false);
              oldAppender = segmentAppender.get();
            } else {
              break;
            }
          }
        };

        wal.addSegmentOverflowListener(listener);

        boolean[] report = new boolean[1];
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
          @Override
          public void run() {
            report[0] = true;
          }
        }, 1000 * 30, 1000 * 30);

        System.out.println("Records load");
        List<TestRecord> records = new ArrayList<>();
        while (!walIsFull.get()) {
          final TestRecord walRecord;
          if (random.nextDouble() < 0.5) {
            walRecord = new TestRecord(random, 4 * wal.pageSize(), 2 * wal.pageSize());
          } else {
            walRecord = new TestRecord(random, 2, 1);
          }
          records.add(walRecord);

          OLogSequenceNumber lsn = wal.log(walRecord);
          Assert.assertEquals(walRecord.getLsn(), lsn);

          Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
          Assert.assertEquals(wal.end(), lsn);

          if (report[0]) {
            System.out.printf("%d records were added, log is filled on %d%%\n", records.size(), 100L * wal.size() / walSizeLimit);
            report[0] = false;
          }
        }

        System.out.println("Records check");
        for (int i = 0; i < records.size(); i++) {
          final List<OWriteableWALRecord> result = wal.read(records.get(i).getLsn(), 10);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i, records.size()).iterator();

          while (resultIterator.hasNext()) {
            TestRecord record = recordIterator.next();
            TestRecord resultRecord = (TestRecord) resultIterator.next();

            Assert.assertArrayEquals(record.data, resultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }

          if (report[0]) {
            System.out.printf("%d records out of %d were checked\n", i, records.size());
            report[0] = false;
          }
        }

        wal.close();
        timer.cancel();
      } catch (Exception | Error e) {
        System.out.println("testAddBigSmallRecords512MSeg : " + seed);
        throw e;
      }
    }
  }

  @Test
  @Ignore
  public void testAddBigMiddleSmallRecords10MSeg() throws Exception {
    final int iterations = 1;
    for (int n = 0; n < iterations; n++) {
      final long seed = System.nanoTime();
      final Random random = new Random(seed);

      try {
        final long walSizeLimit = 10 * 1024 * 1024 * 1024L;
        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64,
            Integer.MAX_VALUE, 10 * 1024 * 1024, 20, true, Locale.US, walSizeLimit, -1, 1000, true, false, true, 10);

        AtomicBoolean walIsFull = new AtomicBoolean();

        final OCheckpointRequestListener checkpointRequestListener = () -> walIsFull.set(true);
        wal.addFullCheckpointListener(checkpointRequestListener);
        ExecutorService executorService = Executors.newCachedThreadPool();

        AtomicReference<Future<Void>> segmentAppender = new AtomicReference<>();

        final OSegmentOverflowListener listener = (segment) -> {
          Future<Void> oldAppender = segmentAppender.get();

          while (oldAppender == null || oldAppender.isDone()) {
            if (wal.activeSegment() <= segment) {
              final Future<Void> appender = executorService.submit(new SegmentAdder(segment, wal));

              if (segmentAppender.compareAndSet(oldAppender, appender)) {
                break;
              }

              appender.cancel(false);
              oldAppender = segmentAppender.get();
            } else {
              break;
            }
          }
        };

        wal.addSegmentOverflowListener(listener);

        boolean[] report = new boolean[1];
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
          @Override
          public void run() {
            report[0] = true;
          }
        }, 1000 * 30, 1000 * 30);

        System.out.println("Records load");
        List<TestRecord> records = new ArrayList<>();
        while (!walIsFull.get()) {
          final TestRecord walRecord;

          final double rnd = random.nextDouble();
          if (rnd < 0.2) {
            walRecord = new TestRecord(random, 4 * wal.pageSize(), 2 * wal.pageSize());
          } else if (rnd < 0.8) {
            walRecord = new TestRecord(random, wal.pageSize(), 100);
          } else {
            walRecord = new TestRecord(random, 2, 1);
          }
          records.add(walRecord);

          OLogSequenceNumber lsn = wal.log(walRecord);
          Assert.assertEquals(walRecord.getLsn(), lsn);

          Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
          Assert.assertEquals(wal.end(), lsn);

          if (report[0]) {
            System.out.printf("%d records were added, log is filled on %d%%\n", records.size(), 100L * wal.size() / walSizeLimit);
            report[0] = false;
          }
        }

        System.out.println("Records check");
        for (int i = 0; i < records.size(); i++) {
          final List<OWriteableWALRecord> result = wal.read(records.get(i).getLsn(), 10);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i, records.size()).iterator();

          while (resultIterator.hasNext()) {
            TestRecord record = recordIterator.next();
            TestRecord resultRecord = (TestRecord) resultIterator.next();

            Assert.assertArrayEquals(record.data, resultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }

          if (report[0]) {
            System.out.printf("%d records out of %d were checked\n", i, records.size());
            report[0] = false;
          }
        }

        wal.close();
        timer.cancel();
      } catch (Exception | Error e) {
        System.out.println("testAddBigMiddleSmallRecords10MSeg : " + seed);
        throw e;
      }
    }
  }

  @Test
  @Ignore
  public void testAddBigMiddleSmallRecords512MSeg() throws Exception {
    final int iterations = 1;
    for (int n = 0; n < iterations; n++) {
      final long seed = System.nanoTime();
      final Random random = new Random(seed);

      try {
        final long walSizeLimit = 10 * 1024 * 1024 * 1024L;
        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64,
            Integer.MAX_VALUE, 512 * 1024 * 1024, 20, true, Locale.US, walSizeLimit, -1, 1000, true, false, true, 10);

        AtomicBoolean walIsFull = new AtomicBoolean();

        final OCheckpointRequestListener checkpointRequestListener = () -> walIsFull.set(true);
        wal.addFullCheckpointListener(checkpointRequestListener);
        ExecutorService executorService = Executors.newCachedThreadPool();

        AtomicReference<Future<Void>> segmentAppender = new AtomicReference<>();

        final OSegmentOverflowListener listener = (segment) -> {
          Future<Void> oldAppender = segmentAppender.get();

          while (oldAppender == null || oldAppender.isDone()) {
            if (wal.activeSegment() <= segment) {
              final Future<Void> appender = executorService.submit(new SegmentAdder(segment, wal));

              if (segmentAppender.compareAndSet(oldAppender, appender)) {
                break;
              }

              appender.cancel(false);
              oldAppender = segmentAppender.get();
            } else {
              break;
            }
          }
        };

        wal.addSegmentOverflowListener(listener);

        boolean[] report = new boolean[1];
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
          @Override
          public void run() {
            report[0] = true;
          }
        }, 1000 * 30, 1000 * 30);

        System.out.println("Records load");
        List<TestRecord> records = new ArrayList<>();
        while (!walIsFull.get()) {
          final TestRecord walRecord;

          final double rnd = random.nextDouble();
          if (rnd < 0.2) {
            walRecord = new TestRecord(random, 4 * wal.pageSize(), 2 * wal.pageSize());
          } else if (rnd < 0.8) {
            walRecord = new TestRecord(random, wal.pageSize(), 100);
          } else {
            walRecord = new TestRecord(random, 2, 1);
          }
          records.add(walRecord);

          OLogSequenceNumber lsn = wal.log(walRecord);
          Assert.assertEquals(walRecord.getLsn(), lsn);

          Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
          Assert.assertEquals(wal.end(), lsn);

          if (report[0]) {
            System.out.printf("%d records were added, log is filled on %d%%\n", records.size(), 100L * wal.size() / walSizeLimit);
            report[0] = false;
          }
        }

        System.out.println("Records check");
        for (int i = 0; i < records.size(); i++) {
          final List<OWriteableWALRecord> result = wal.read(records.get(i).getLsn(), 10);
          Assert.assertTrue(!result.isEmpty());

          final Iterator<OWriteableWALRecord> resultIterator = result.iterator();
          final Iterator<TestRecord> recordIterator = records.subList(i, records.size()).iterator();

          while (resultIterator.hasNext()) {
            TestRecord record = recordIterator.next();
            TestRecord resultRecord = (TestRecord) resultIterator.next();

            Assert.assertArrayEquals(record.data, resultRecord.data);
            Assert.assertEquals(record.getLsn(), resultRecord.getLsn());
          }

          if (report[0]) {
            System.out.printf("%d records out of %d were checked\n", i, records.size());
            report[0] = false;
          }
        }

        wal.close();
        timer.cancel();
      } catch (Exception | Error e) {
        System.out.println("testAddBigMiddleSmallRecords512MSeg : " + seed);
        throw e;
      }
    }
  }

  @Test
  public void testWALCrash() throws Exception {
    final int iterations = 1;

    for (int n = 0; n < iterations; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      final long seed = System.nanoTime();
      final Random random = new Random(seed);

      try {
        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64,
            Integer.MAX_VALUE, 10 * 1024 * 1024, 20, true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

        ExecutorService executorService = Executors.newCachedThreadPool();

        AtomicReference<Future<Void>> segmentAppender = new AtomicReference<>();

        final OSegmentOverflowListener listener = (segment) -> {
          Future<Void> oldAppender = segmentAppender.get();

          while (oldAppender == null || oldAppender.isDone()) {
            if (wal.activeSegment() <= segment) {
              final Future<Void> appender = executorService.submit(new SegmentAdder(segment, wal));

              if (segmentAppender.compareAndSet(oldAppender, appender)) {
                break;
              }

              appender.cancel(false);
              oldAppender = segmentAppender.get();
            } else {
              break;
            }
          }
        };

        wal.addSegmentOverflowListener(listener);

        final List<TestRecord> records = new ArrayList<>();
        final int recordsCount = 100_000;
        for (int i = 0; i < recordsCount; i++) {
          final TestRecord walRecord = new TestRecord(random, 3 * wal.pageSize(), 1);
          wal.log(walRecord);
          records.add(walRecord);
        }

        wal.close();

        final int index = random.nextInt(records.size());
        final OLogSequenceNumber lsn = records.get(index).getLsn();
        final long segment = lsn.getSegment();
        final long page = lsn.getPosition() / wal.pageSize();

        try (final FileChannel channel = FileChannel
            .open(testDirectory.resolve(getSegmentName(segment)), StandardOpenOption.WRITE, StandardOpenOption.READ)) {
          channel.position(page * wal.pageSize());

          final ByteBuffer buffer = ByteBuffer.allocate(wal.pageSize());
          channel.read(buffer);

          buffer.put(42, (byte) (buffer.get(42) + 1));
          buffer.position(0);

          channel.write(buffer);
        }

        OCASDiskWriteAheadLog loadedWAL = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64,
            Integer.MAX_VALUE, 10 * 1024 * 1024, 20, true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

        Iterator<TestRecord> recordIterator = records.iterator();
        List<OWriteableWALRecord> walRecords = loadedWAL.read(records.get(0).getLsn(), 100);
        Iterator<OWriteableWALRecord> walRecordIterator = walRecords.iterator();

        OLogSequenceNumber lastLSN = null;
        int recordCounter = 0;

        if (segment == 1 && page == 0) {
          Assert.assertTrue(walRecords.isEmpty());
        } else {
          while (recordIterator.hasNext()) {
            if (walRecordIterator.hasNext()) {
              final OWriteableWALRecord walRecord = walRecordIterator.next();

              final TestRecord walTestRecord = (TestRecord) walRecord;
              final TestRecord record = recordIterator.next();

              Assert.assertEquals(record.getLsn(), walTestRecord.getLsn());
              Assert.assertArrayEquals(record.data, walTestRecord.data);

              lastLSN = record.getLsn();

              recordCounter++;
            } else {
              walRecords = loadedWAL.next(lastLSN, 100);

              if (walRecords.isEmpty()) {
                break;
              }

              walRecordIterator = walRecords.iterator();
            }
          }
        }

        final OLogSequenceNumber nextRecordLSN = records.get(recordCounter).getLsn();
        Assert.assertEquals(segment, nextRecordLSN.getSegment());
        Assert.assertTrue(page >= nextRecordLSN.getPosition() / wal.pageSize());

        loadedWAL.close();

        System.out.printf("%d iterations out of %d were passed\n", n, iterations);
      } catch (Exception | Error e) {
        System.out.println("testWALCrash seed : " + seed);
        throw e;
      }
    }
  }

  @Test
  public void testMasterRecordCrashTwoRecords() throws Exception {
    OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64, Integer.MAX_VALUE,
        10 * 1024 * 1024, 20, true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

    OLogSequenceNumber masterLSNOne = wal.log(new OFullCheckpointStartRecord());
    wal.flush();

    OLogSequenceNumber masterLSNTwo = wal.log(new OFullCheckpointStartRecord());
    wal.flush();

    Assert.assertEquals(masterLSNTwo, wal.lastCheckpoint());

    wal.close();

    try (FileChannel wmrChannel = FileChannel
        .open(testDirectory.resolve("walTest.wmr"), StandardOpenOption.READ, StandardOpenOption.WRITE)) {
      final ByteBuffer buffer = ByteBuffer.allocate(20);

      wmrChannel.position(20);
      OIOUtils.readByteBuffer(buffer, wmrChannel);

      buffer.put(10, (byte) (buffer.get(10) + 1));

      buffer.position(0);
      OIOUtils.writeByteBuffer(buffer, wmrChannel, 20);
    }

    wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64, Integer.MAX_VALUE, 10 * 1024 * 1024, 20,
        true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

    Assert.assertEquals(masterLSNOne, wal.lastCheckpoint());

    wal.close();
  }

  @Test
  public void testMasterRecordCrashThreeRecordsFirstDamage() throws Exception {
    OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64, Integer.MAX_VALUE,
        10 * 1024 * 1024, 20, true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

    wal.log(new OFullCheckpointStartRecord());
    wal.flush();

    OLogSequenceNumber masterLSNOne = wal.log(new OFullCheckpointStartRecord());
    wal.flush();

    OLogSequenceNumber masterLSNTwo = wal.log(new OFullCheckpointStartRecord());
    wal.flush();

    Assert.assertEquals(masterLSNTwo, wal.lastCheckpoint());

    wal.close();

    try (FileChannel wmrChannel = FileChannel
        .open(testDirectory.resolve("walTest.wmr"), StandardOpenOption.READ, StandardOpenOption.WRITE)) {
      final ByteBuffer buffer = ByteBuffer.allocate(20);

      wmrChannel.position(0);
      OIOUtils.readByteBuffer(buffer, wmrChannel);

      buffer.put(10, (byte) (buffer.get(10) + 1));

      buffer.position(0);
      OIOUtils.writeByteBuffer(buffer, wmrChannel, 0);
    }

    wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64, Integer.MAX_VALUE, 10 * 1024 * 1024, 20, true,
        Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

    Assert.assertEquals(masterLSNOne, wal.lastCheckpoint());

    wal.close();
  }

  @Test
  public void testMasterRecordCrashThreeRecordsSecondDamage() throws Exception {
    OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64, Integer.MAX_VALUE,
        10 * 1024 * 1024, 20, true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

    wal.log(new OFullCheckpointStartRecord());
    wal.flush();

    wal.log(new OFullCheckpointStartRecord());
    wal.flush();

    OLogSequenceNumber masterLSNTwo = wal.log(new OFullCheckpointStartRecord());
    wal.flush();

    Assert.assertEquals(masterLSNTwo, wal.lastCheckpoint());

    wal.close();

    try (FileChannel wmrChannel = FileChannel
        .open(testDirectory.resolve("walTest.wmr"), StandardOpenOption.READ, StandardOpenOption.WRITE)) {
      final ByteBuffer buffer = ByteBuffer.allocate(20);

      wmrChannel.position(20);
      OIOUtils.readByteBuffer(buffer, wmrChannel);

      buffer.put(10, (byte) (buffer.get(10) + 1));

      buffer.position(0);
      OIOUtils.writeByteBuffer(buffer, wmrChannel, 20);
    }

    wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 64, Integer.MAX_VALUE, 10 * 1024 * 1024, 20, true,
        Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000, true, false, true, 10);

    Assert.assertEquals(masterLSNTwo, wal.lastCheckpoint());

    wal.close();
  }

  private void checkThatSegmentsBellowAreRemoved(OCASDiskWriteAheadLog wal) {
    final OLogSequenceNumber begin = wal.begin();

    for (int i = 0; i < begin.getSegment(); i++) {
      final Path segmentPath = testDirectory.resolve(getSegmentName(i));
      Assert.assertFalse(Files.exists(segmentPath));
    }
  }

  private void checkThatAllNonActiveSegmentsAreRemoved(long[] nonActive, long segment, OCASDiskWriteAheadLog wal) {
    if (nonActive.length == 0) {
      return;
    }

    final int index = Arrays.binarySearch(nonActive, segment);
    final OLogSequenceNumber begin = wal.begin();

    if (index < 0) {
      Assert.assertTrue(begin.getSegment() > nonActive[nonActive.length - 1]);
    } else {
      Assert.assertTrue(begin.getSegment() >= nonActive[index]);
    }
  }

  private static void addLimit(TreeMap<OLogSequenceNumber, Integer> limits, OLogSequenceNumber lsn) {
    limits.merge(lsn, 1, (a, b) -> a + b);
  }

  private static void removeLimit(TreeMap<OLogSequenceNumber, Integer> limits, OLogSequenceNumber lsn) {
    Integer counter = limits.get(lsn);
    if (counter == 1) {
      limits.remove(lsn);
    } else {
      limits.put(lsn, counter - 1);
    }
  }

  private static OLogSequenceNumber chooseRandomRecord(Random random,
      NavigableMap<OLogSequenceNumber, ? extends OWriteableWALRecord> records) {
    if (records.isEmpty()) {
      return null;
    }
    OLogSequenceNumber first = records.firstKey();
    OLogSequenceNumber last = records.lastKey();

    final int firstSegment = (int) first.getSegment();
    final int lastSegment = (int) last.getSegment();

    final int segment;
    if (lastSegment > firstSegment) {
      segment = random.nextInt(lastSegment - firstSegment) + firstSegment;
    } else {
      segment = lastSegment;
    }

    final OLogSequenceNumber lastLSN = records.floorKey(new OLogSequenceNumber(segment, Integer.MAX_VALUE));
    final int position = random.nextInt((int) lastLSN.getPosition());

    OLogSequenceNumber lsn = records.ceilingKey(new OLogSequenceNumber(segment, position));
    Assert.assertNotNull(lsn);

    return lsn;
  }

  private void assertMTWALInsertion(OCASDiskWriteAheadLog wal,
      SortedMap<OLogSequenceNumber, ? extends OWriteableWALRecord> addedRecords) throws IOException {
    final Iterator<? extends OWriteableWALRecord> recordsIterator = addedRecords.values().iterator();

    OWriteableWALRecord record = recordsIterator.next();
    List<OWriteableWALRecord> records = wal.read(record.getLsn(), 100);
    Iterator<OWriteableWALRecord> readIterator = records.iterator();
    OWriteableWALRecord readRecord = readIterator.next();

    Assert.assertEquals(record.getLsn(), readRecord.getLsn());
    if (record instanceof TestRecord) {
      Assert.assertArrayEquals(((TestRecord) record).data, ((TestRecord) readRecord).data);
    }

    OLogSequenceNumber lastLSN = record.getLsn();
    while (readIterator.hasNext()) {
      readRecord = readIterator.next();
      record = recordsIterator.next();

      Assert.assertEquals(record.getLsn(), readRecord.getLsn());

      if (record instanceof TestRecord) {
        Assert.assertArrayEquals(((TestRecord) record).data, ((TestRecord) readRecord).data);
      }
      lastLSN = record.getLsn();
    }

    while (recordsIterator.hasNext()) {
      records = wal.next(lastLSN, 100);
      Assert.assertTrue(!records.isEmpty());

      readIterator = records.iterator();
      while (readIterator.hasNext() && recordsIterator.hasNext()) {
        readRecord = readIterator.next();
        record = recordsIterator.next();

        Assert.assertEquals(record.getLsn(), readRecord.getLsn());
        try {
          if (record instanceof TestRecord) {
            Assert.assertArrayEquals("Record LSN " + record.getLsn() + ", record data length " + ((TestRecord) record).data.length,
                ((TestRecord) record).data, ((TestRecord) readRecord).data);
          }
        } catch (AssertionError e) {
          if (readIterator.hasNext()) {
            OWriteableWALRecord r = readIterator.next();
            if (r instanceof TestRecord) {
              System.out.println("Next read LSN is " + r.getLsn() + " record size is " + ((TestRecord) r).data.length);
            }
          }

          if (recordsIterator.hasNext()) {
            OWriteableWALRecord r = recordsIterator.next();
            if (r instanceof TestRecord) {
              System.out.println("Next stored LSN is " + r.getLsn() + " record size is " + ((TestRecord) r).data.length);
            }
          }
          throw e;
        }
        lastLSN = record.getLsn();
      }
    }

    Assert.assertTrue(!recordsIterator.hasNext());
  }

  private String getSegmentName(long segment) {
    return "walTest." + segment + ".wal";
  }

  private static final class SegmentAdder implements Callable<Void> {
    private final long                  segment;
    private final OCASDiskWriteAheadLog wal;

    SegmentAdder(long segment, OCASDiskWriteAheadLog wal) {
      this.segment = segment;
      this.wal = wal;
    }

    @Override
    public Void call() {
      try {
        wal.appendSegment(segment + 1);
      } catch (Exception | Error e) {
        e.printStackTrace();
        throw e;
      }

      return null;
    }
  }

  public static final class TestRecord extends OAbstractWALRecord {
    private byte[] data;

    @SuppressWarnings("unused")
    public TestRecord() {
    }

    @SuppressWarnings("unused")
    public TestRecord(byte[] data) {
      this.data = data;
    }

    TestRecord(Random random, int maxSize, int minSize) {
      int len = random.nextInt(maxSize - minSize + 1) + 1;
      data = new byte[len];
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
    public void toStream(ByteBuffer buffer) {
      buffer.putInt(data.length);
      buffer.put(data);
    }

    @Override
    public int fromStream(byte[] content, int offset) {
      int len = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      data = new byte[len];
      System.arraycopy(content, offset, data, 0, len);
      offset += len;

      return offset;
    }

    @Override
    public int serializedSize() {
      return data.length + OIntegerSerializer.INT_SIZE;
    }

    @Override
    public boolean isUpdateMasterRecord() {
      return false;
    }

    @Override
    public byte getId() {
      return 127;
    }
  }

  public static final class RecordsAdder implements Callable<List<TestRecord>> {
    private final OCASDiskWriteAheadLog wal;
    private final AtomicBoolean         walIsFull;
    private final List<TestRecord>      addedRecords = new ArrayList<>();

    RecordsAdder(OCASDiskWriteAheadLog wal, AtomicBoolean walIsFull) {
      this.wal = wal;
      this.walIsFull = walIsFull;
    }

    @Override
    public List<TestRecord> call() throws Exception {
      try {
        final ThreadLocalRandom random = ThreadLocalRandom.current();

        while (!walIsFull.get()) {
          final int batchSize = random.nextInt(11) + 10;

          for (int i = 0; i < batchSize; i++) {
            final TestRecord record = new TestRecord(random, 4 * wal.pageSize(), 1);
            final OLogSequenceNumber lsn = wal.log(record);

            Assert.assertEquals(lsn, record.getLsn());

            addedRecords.add(record);
          }

          if (random.nextDouble() < 0.2) {
            assertWAL(random);
          }
        }
      } catch (Exception | Error e) {
        e.printStackTrace();
        throw e;
      }

      return addedRecords;
    }

    private void assertWAL(ThreadLocalRandom random) throws IOException {
      final int startIndex = random.nextInt(addedRecords.size());
      final int checkInterval = random.nextInt(81) + 20;

      List<TestRecord> records = addedRecords.subList(startIndex, Math.min(addedRecords.size(), startIndex + checkInterval));
      OLogSequenceNumber lsn = records.get(0).getLsn();

      List<OWriteableWALRecord> readRecords = wal.read(lsn, 10);
      OLogSequenceNumber callLSN = lsn;

      Iterator<OWriteableWALRecord> readIterator = readRecords.iterator();

      OLogSequenceNumber lastLSN = null;

      for (TestRecord record : records) {
        OLogSequenceNumber recordLSN = record.getLsn();

        while (true) {
          if (readIterator.hasNext()) {
            OWriteableWALRecord walRecord = readIterator.next();
            OLogSequenceNumber walRecordLSN = walRecord.getLsn();

            lastLSN = walRecordLSN;

            final int compare = walRecordLSN.compareTo(recordLSN);
            if (compare < 0) {
              //noinspection UnnecessaryContinue
              continue;
            } else if (compare == 0) {
              Assert.assertArrayEquals("Call LSN " + callLSN + ", record LSN " + recordLSN + ", record length " + record.data.length
                  + ", wal record length" + ((TestRecord) walRecord).data.length + ", record distance " + record.getDistance()
                  + ", record size " + record.getDiskSize(), record.data, ((TestRecord) walRecord).data);
              break;
            } else {
              Assert.fail("Call LSN " + callLSN + ", record LSN " + recordLSN + ", WAL record LSN " + walRecordLSN);
            }

          } else {
            Assert.assertNotNull("Call LSN " + callLSN, lastLSN);
            readRecords = wal.next(lastLSN, 10);
            callLSN = lastLSN;

            readIterator = readRecords.iterator();

            Assert.assertTrue("Call LSN " + callLSN, readIterator.hasNext());
          }
        }

      }
    }
  }

  public static final class RecordsAdderBackwardIteration implements Callable<List<TestRecord>> {
    private final OCASDiskWriteAheadLog wal;
    private final AtomicBoolean         walIsFull;
    private final List<TestRecord>      addedRecords = new ArrayList<>();

    RecordsAdderBackwardIteration(OCASDiskWriteAheadLog wal, AtomicBoolean walIsFull) {
      this.wal = wal;
      this.walIsFull = walIsFull;
    }

    @Override
    public List<TestRecord> call() throws Exception {
      try {
        final ThreadLocalRandom random = ThreadLocalRandom.current();

        while (!walIsFull.get()) {
          final int batchSize = random.nextInt(11) + 10;

          for (int i = 0; i < batchSize; i++) {
            final TestRecord record = new TestRecord(random, 4 * wal.pageSize(), 1);
            final OLogSequenceNumber lsn = wal.log(record);

            Assert.assertEquals(lsn, record.getLsn());

            addedRecords.add(record);
          }

          if (random.nextDouble() < 0.2) {
            assertWAL(random);
          }
        }
      } catch (Exception | Error e) {
        e.printStackTrace();
        throw e;
      }

      return addedRecords;
    }

    private void assertWAL(ThreadLocalRandom random) throws IOException {
      final int checkInterval = random.nextInt(81) + 20;

      final int startIndex = Math.max(0, addedRecords.size() - checkInterval);

      List<TestRecord> records = addedRecords.subList(startIndex, Math.min(startIndex + checkInterval, addedRecords.size()));
      OLogSequenceNumber lsn = records.get(0).getLsn();

      List<OWriteableWALRecord> readRecords = wal.read(lsn, 10);
      OLogSequenceNumber callLSN = lsn;
      Iterator<OWriteableWALRecord> readIterator = readRecords.iterator();

      OLogSequenceNumber lastLSN = null;

      for (TestRecord record : records) {
        OLogSequenceNumber recordLSN = record.getLsn();

        while (true) {
          if (readIterator.hasNext()) {
            OWriteableWALRecord walRecord = readIterator.next();
            OLogSequenceNumber walRecordLSN = walRecord.getLsn();

            lastLSN = walRecordLSN;

            final int compare = walRecordLSN.compareTo(recordLSN);
            if (compare < 0) {
              //noinspection UnnecessaryContinue
              continue;
            } else if (compare == 0) {
              Assert.assertArrayEquals("Call LSN " + callLSN + ", record LSN " + recordLSN + ", record length " + record.data.length
                  + ", wal record length" + ((TestRecord) walRecord).data.length + ", record distance " + record.getDistance()
                  + ", record size " + record.getDiskSize(), record.data, ((TestRecord) walRecord).data);

              break;
            } else {
              Assert.fail("Call LSN " + callLSN + ", record LSN " + recordLSN + ", WAL record LSN " + walRecordLSN);
            }

          } else {
            Assert.assertNotNull("Call LSN " + callLSN, lastLSN);
            readRecords = wal.next(lastLSN, 10);
            callLSN = lastLSN;
            readIterator = readRecords.iterator();

            Assert.assertTrue("Call LSN " + callLSN, readIterator.hasNext());
          }
        }

      }
    }
  }

  private static final class CutTillTester implements Callable<TreeMap<OLogSequenceNumber, OWriteableWALRecord>> {
    private final OCASDiskWriteAheadLog wal;
    private final AtomicBoolean         walIsFull;

    private final TreeMap<OLogSequenceNumber, OWriteableWALRecord> addedRecords = new TreeMap<>();
    private final TreeMap<OLogSequenceNumber, Integer>             limits       = new TreeMap<>();
    private final AtomicBoolean                                    sendReport;

    private OLogSequenceNumber cutLimit;

    CutTillTester(OCASDiskWriteAheadLog wal, AtomicBoolean walIsFull, AtomicBoolean sendReport) {
      this.wal = wal;
      this.walIsFull = walIsFull;
      this.sendReport = sendReport;
    }

    @Override
    public TreeMap<OLogSequenceNumber, OWriteableWALRecord> call() throws Exception {
      final ThreadLocalRandom random = ThreadLocalRandom.current();

      int failures = 0;
      while (!walIsFull.get()) {
        if (random.nextDouble() <= 0.2) {
          if (limits.isEmpty() || random.nextDouble() <= 0.5) {
            addLimit(random);
          } else {
            removeLimit(random);
          }

          calculateLimit();
        } else {
          final TestRecord record = new TestRecord(random, 2 * wal.pageSize(), 1);
          final OLogSequenceNumber lsn = wal.log(record);
          addedRecords.put(lsn, record);
        }

        final OLogSequenceNumber begin = wal.begin();
        final OLogSequenceNumber end = wal.end();

        boolean result;
        if (begin.getSegment() != end.getSegment()) {
          final int segment = (int) (random.nextInt((int) (end.getSegment() - begin.getSegment())) + begin.getSegment());
          result = wal.cutAllSegmentsSmallerThan(segment);
        } else {
          result = wal.cutAllSegmentsSmallerThan(begin.getSegment());
        }

        if (!result) {
          failures++;
        }

        if (cutLimit != null) {
          Assert.assertTrue(wal.begin().compareTo(cutLimit) <= 0);
        }

        clearRecords();
        if (sendReport.get()) {
          System.out.println("Thread " + Thread.currentThread().getId() + ", cut till limit " + cutLimit);
          sendReport.set(false);
        }
      }

      System.out.println("Thread " + Thread.currentThread().getId() + ", failures " + failures);
      return addedRecords;
    }

    private void calculateLimit() {
      final OLogSequenceNumber begin = wal.begin();
      OLogSequenceNumber cutLimit = null;

      if (!limits.isEmpty()) {
        cutLimit = limits.firstKey();
      }

      if (cutLimit != null) {
        if (begin.compareTo(cutLimit) > 0) {
          this.cutLimit = begin;
        } else {
          this.cutLimit = cutLimit;
        }
      } else {
        this.cutLimit = null;
      }
    }

    private void clearRecords() {
      final OLogSequenceNumber begin = wal.begin();
      addedRecords.headMap(begin, false).clear();
    }

    private void addLimit(ThreadLocalRandom random) {
      clearRecords();

      if (addedRecords.isEmpty()) {
        return;
      }

      final OLogSequenceNumber lsn = chooseRandomRecord(random, addedRecords);
      if (lsn == null) {
        return;
      }

      wal.addCutTillLimit(lsn);
      OCASDiskWriteAheadLogIT.addLimit(limits, lsn);
    }

    private void removeLimit(ThreadLocalRandom random) {
      final int size = limits.size();
      final int number = random.nextInt(size);
      final Iterator<OLogSequenceNumber> limitsIterator = limits.keySet().iterator();

      for (int i = 0; i < number - 1; i++) {
        limitsIterator.next();
      }

      final OLogSequenceNumber limit = limitsIterator.next();
      wal.removeCutTillLimit(limit);
      OCASDiskWriteAheadLogIT.removeLimit(limits, limit);
    }
  }
}
