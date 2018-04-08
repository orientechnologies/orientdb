package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OCheckpointRequestListener;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAbstractWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecordsFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class OCASDiskWriteAheadLogTest {
  private static Path testDirectory;

  @BeforeClass
  public static void beforeClass() {
    testDirectory = Paths.get(System.getProperty("buildDirectory" + File.separator + "casWALTest",
        "." + File.separator + "target" + File.separator + "casWALTest"));

    OWALRecordsFactory.INSTANCE.registerNewRecord((byte) 128, TestRecord.class);
  }

  @Before
  public void before() {
    OFileUtils.deleteRecursively(testDirectory.toFile());
  }

  @Test
  public void testAddSingleOnePageRecord() throws Exception {
    final int iterations = 100_000;

    for (int i = 0; i < iterations; i++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      final long seed = System.nanoTime();
      try {
        final Random random = new Random(seed);

        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, Integer.MAX_VALUE, 1000,
            true, Locale.US, -1, -1, 1000);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

        TestRecord walRecord = new TestRecord(random, OCASWALPage.PAGE_SIZE, 1);
        final OLogSequenceNumber lsn = wal.log(walRecord);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), lsn);

        List<OWriteableWALRecord> records = wal.read(lsn, 10);
        Assert.assertEquals(1, records.size());
        TestRecord readRecord = (TestRecord) records.get(0);

        Assert.assertArrayEquals(walRecord.data, readRecord.data);
        Assert.assertEquals(lsn, walRecord.getLsn());
        wal.close();

        wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, Integer.MAX_VALUE, 1000, true, Locale.US, -1,
            -1, 1000);

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
    final int iterations = 30_000;
    for (int i = 0; i < iterations; i++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      final long seed = System.nanoTime();
      try {
        final Random random = new Random(seed);

        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, Integer.MAX_VALUE, 1000,
            true, Locale.US, -1, -1, 1000);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

        TestRecord walRecord = new TestRecord(random, 2 * OCASWALPage.PAGE_SIZE, OCASWALPage.PAGE_SIZE);
        final OLogSequenceNumber lsn = wal.log(walRecord);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), lsn);

        List<OWriteableWALRecord> records = wal.read(lsn, 10);
        Assert.assertEquals(1, records.size());
        TestRecord readRecord = (TestRecord) records.get(0);

        Assert.assertArrayEquals(walRecord.data, readRecord.data);
        Assert.assertEquals(lsn, walRecord.getLsn());
        wal.close();

        wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, Integer.MAX_VALUE, 1000, true, Locale.US, -1,
            -1, 1000);

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
        System.out.println("testAddSingleRecordSeveralPages : " + seed);
        throw e;
      }
    }
  }

  @Test
  public void testAddFewSmallRecords() throws Exception {
    final int iterations = 100_000;
    for (int n = 0; n < iterations; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      final long seed = System.nanoTime();
      try {
        final Random random = new Random(seed);

        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, Integer.MAX_VALUE, 1000,
            true, Locale.US, -1, -1, 1000);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

        List<TestRecord> records = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
          final TestRecord walRecord = new TestRecord(random, OCASWALPage.PAGE_SIZE, 1);
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

        wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, Integer.MAX_VALUE, 1000, true, Locale.US, -1,
            -1, 1000);

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
    final int iterations = 100_000;

    for (int n = 0; n < iterations; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      final long seed = System.nanoTime();
      try {
        final Random random = new Random(seed);

        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, Integer.MAX_VALUE, 1000,
            true, Locale.US, -1, -1, 1000);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

        List<TestRecord> records = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
          final TestRecord walRecord = new TestRecord(random, OCASWALPage.PAGE_SIZE, 1);
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

        wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, Integer.MAX_VALUE, 1000, true, Locale.US, -1,
            -1, 1000);

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
    final int iterations = 30_000;

    for (int n = 0; n < iterations; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      final long seed = System.nanoTime();
      try {
        final Random random = new Random(seed);

        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, Integer.MAX_VALUE, 1000,
            true, Locale.US, -1, -1, 1000);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

        List<TestRecord> records = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
          final TestRecord walRecord = new TestRecord(random, 2 * OCASWALPage.PAGE_SIZE, OCASWALPage.PAGE_SIZE);
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

        wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, Integer.MAX_VALUE, 1000, true, Locale.US, -1,
            -1, 1000);

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
    final int iterations = 30_000;

    for (int n = 0; n < iterations; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      final long seed = System.nanoTime();
      try {
        final Random random = new Random(seed);

        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, Integer.MAX_VALUE, 1000,
            true, Locale.US, -1, -1, 1000);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

        List<TestRecord> records = new ArrayList<>();

        for (int i = 0; i < 5; i++) {
          final TestRecord walRecord = new TestRecord(random, 2 * OCASWALPage.PAGE_SIZE, OCASWALPage.PAGE_SIZE);
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

        wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, Integer.MAX_VALUE, 1000, true, Locale.US, -1,
            -1, 1000);

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
    final int iterations = 200;

    for (int n = 0; n < iterations; n++) {
      final long seed = System.nanoTime();

      OFileUtils.deleteRecursively(testDirectory.toFile());
      try {
        final Random random = new Random(seed);

        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, Integer.MAX_VALUE, 1000,
            true, Locale.US, -1, -1, 1000);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

        List<TestRecord> records = new ArrayList<>();

        final int recordsCount = 10_000;

        for (int i = 0; i < recordsCount; i++) {
          final TestRecord walRecord = new TestRecord(random, OCASWALPage.PAGE_SIZE, 1);
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

        wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, Integer.MAX_VALUE, 1000, true, Locale.US, -1,
            -1, 1000);

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
    final int iterations = 200;

    for (int n = 0; n < iterations; n++) {
      final long seed = System.nanoTime();

      OFileUtils.deleteRecursively(testDirectory.toFile());
      try {
        final Random random = new Random(seed);

        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, Integer.MAX_VALUE, 1000,
            true, Locale.US, -1, -1, 1000);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

        List<TestRecord> records = new ArrayList<>();

        final int recordsCount = 10_000;

        for (int i = 0; i < recordsCount; i++) {
          final TestRecord walRecord = new TestRecord(random, OCASWALPage.PAGE_SIZE, 1);
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

        wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, Integer.MAX_VALUE, 1000, true, Locale.US, -1,
            -1, 1000);

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
    int iterations = 300;

    for (int n = 0; n < iterations; n++) {
      final long seed = System.nanoTime();
      final Random random = new Random(seed);

      OFileUtils.deleteRecursively(testDirectory.toFile());
      try {
        final int numberOfSegmentsToAdd = random.nextInt(4) + 3;

        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, Integer.MAX_VALUE, 1000,
            true, Locale.US, -1, -1, 1000);

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
            final TestRecord walRecord = new TestRecord(random, 2 * OCASWALPage.PAGE_SIZE, OCASWALPage.PAGE_SIZE);
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

        long calculatedWalSize = ((wal.size() + OCASWALPage.PAGE_SIZE - 1) / OCASWALPage.PAGE_SIZE) * OCASWALPage.PAGE_SIZE;

        Assert.assertEquals(calculatedWalSize, walSize);

        wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, Integer.MAX_VALUE, 1000, true, Locale.US, -1,
            -1, 1000);

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
          final TestRecord walRecord = new TestRecord(random, 2 * OCASWALPage.PAGE_SIZE, OCASWALPage.PAGE_SIZE);
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

        calculatedWalSize = ((wal.size() + OCASWALPage.PAGE_SIZE - 1) / OCASWALPage.PAGE_SIZE) * OCASWALPage.PAGE_SIZE;

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
    final int iterations = 200;

    for (int n = 0; n < iterations; n++) {
      final long seed = System.nanoTime();

      OFileUtils.deleteRecursively(testDirectory.toFile());

      try {
        final Random random = new Random(seed);

        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, Integer.MAX_VALUE, 1000,
            true, Locale.US, -1, -1, 1000);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

        List<TestRecord> records = new ArrayList<>();

        final int recordsCount = 10_000;

        for (int i = 0; i < recordsCount; i++) {
          final TestRecord walRecord = new TestRecord(random, 2 * OCASWALPage.PAGE_SIZE, OCASWALPage.PAGE_SIZE);
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

        wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, Integer.MAX_VALUE, 1000, true, Locale.US, -1,
            -1, 1000);

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
    final int iterations = 200;

    for (int n = 0; n < iterations; n++) {
      final long seed = System.nanoTime();

      OFileUtils.deleteRecursively(testDirectory.toFile());

      try {
        final Random random = new Random(seed);

        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, Integer.MAX_VALUE, 1000,
            true, Locale.US, -1, -1, 1000);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

        List<TestRecord> records = new ArrayList<>();

        final int recordsCount = 10_000;

        for (int i = 0; i < recordsCount; i++) {
          final TestRecord walRecord = new TestRecord(random, 2 * OCASWALPage.PAGE_SIZE, OCASWALPage.PAGE_SIZE);
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

        wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, Integer.MAX_VALUE, 1000, true, Locale.US, -1,
            -1, 1000);

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
    final int iterations = 200;
    for (int n = 0; n < iterations; n++) {
      final long seed = System.nanoTime();

      OFileUtils.deleteRecursively(testDirectory.toFile());

      try {
        final Random random = new Random(seed);

        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, Integer.MAX_VALUE, 1000,
            true, Locale.US, -1, -1, 1000);

        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

        List<TestRecord> records = new ArrayList<>();

        final int recordsCount = 10_000;

        for (int i = 0; i < recordsCount; i++) {
          final TestRecord walRecord = new TestRecord(random, 3 * OCASWALPage.PAGE_SIZE, 1);
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

        wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, Integer.MAX_VALUE, 1000, true, Locale.US, -1,
            -1, 1000);

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
    final int iterations = 200;
    for (int n = 0; n < iterations; n++) {
      final long seed = System.nanoTime();

      OFileUtils.deleteRecursively(testDirectory.toFile());

      try {
        final Random random = new Random(seed);

        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, Integer.MAX_VALUE, 1000,
            true, Locale.US, -1, -1, 1000);
        Assert.assertEquals(wal.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
        Assert.assertEquals(wal.end(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));

        List<TestRecord> records = new ArrayList<>();

        final int recordsCount = 10_000;

        for (int i = 0; i < recordsCount; i++) {
          final TestRecord walRecord = new TestRecord(random, 3 * OCASWALPage.PAGE_SIZE, 1);
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

        wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, Integer.MAX_VALUE, 1000, true, Locale.US, -1,
            -1, 1000);

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
    final int iterations = 1500;

    for (int n = 0; n < iterations; n++) {
      final long seed = System.nanoTime();
      OFileUtils.deleteRecursively(testDirectory.toFile());

      try {
        final Random random = new Random(seed);
        final int recordsCount = random.nextInt(10_000) + 100;

        OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, Integer.MAX_VALUE, 1000,
            true, Locale.US, -1, -1, 1000);
        for (int i = 0; i < recordsCount; i++) {

          final TestRecord testRecord = new TestRecord(random, 4 * OCASWALPage.PAGE_SIZE, 1);
          wal.log(testRecord);
        }

        wal.close();

        final long segSize = Files.walk(testDirectory)
            .filter(p -> p.toFile().isFile() && p.getFileName().toString().endsWith(".wal")).mapToLong(p -> p.toFile().length())
            .sum();

        final long calculatedSegSize =
            ((wal.segSize() + OCASWALPage.PAGE_SIZE - 1) / OCASWALPage.PAGE_SIZE) * OCASWALPage.PAGE_SIZE;
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
  public void appendMT10MSegSmallCacheTest() throws Exception {
    final int iterations = 240;
    for (int n = 0; n < iterations; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 10 * 1024 * 1024, 1000,
          true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000);

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

      OCASDiskWriteAheadLog loadedWAL = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 10 * 1024 * 1024,
          1000, true, Locale.US, 3 * 1024 * 1024 * 1024L, -1, 1000);

      System.out.println("Assert WAL content 2");
      assertMTWALInsertion(loadedWAL, addedRecords);

      Assert.assertEquals(loadedWAL.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(loadedWAL.end(), new OLogSequenceNumber(lastLSN.getSegment() + 1, OCASWALPage.RECORDS_OFFSET));

      loadedWAL.close();

      System.out.printf("%d iteration out of %d is passed\n", n, iterations);
    }
  }

  @Test
  public void appendMT10MSegSmallCacheBackwardTest() throws Exception {
    final int iterations = 240;
    for (int n = 0; n < iterations; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 10 * 1024 * 1024, 1000,
          true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000);

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

      OCASDiskWriteAheadLog loadedWAL = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 10 * 1024 * 1024,
          1000, true, Locale.US, 3 * 1024 * 1024 * 1024L, -1, 1000);

      System.out.println("Assert WAL content 2");
      assertMTWALInsertion(loadedWAL, addedRecords);

      Assert.assertEquals(loadedWAL.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(loadedWAL.end(), new OLogSequenceNumber(addedRecords.lastKey().getSegment() + 1, OCASWALPage.RECORDS_OFFSET));

      loadedWAL.close();

      System.out.printf("%d iteration out of %d is passed\n", n, iterations);
    }
  }

  @Test
  public void appendMT10MSegBigCacheTest() throws Exception {
    final int iterations = 240;
    for (int n = 0; n < iterations; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 10 * 1024 * 1024, 1000,
          true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000);

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

      OCASDiskWriteAheadLog loadedWAL = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 10 * 1024 * 1024,
          1000, true, Locale.US, 3 * 1024 * 1024 * 1024L, -1, 1000);

      System.out.println("Assert WAL content 2");
      assertMTWALInsertion(loadedWAL, addedRecords);

      Assert.assertEquals(loadedWAL.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(loadedWAL.end(), new OLogSequenceNumber(addedRecords.lastKey().getSegment() + 1, OCASWALPage.RECORDS_OFFSET));

      loadedWAL.close();

      System.out.printf("%d iteration out of %d is passed\n", n, iterations);
    }
  }

  @Test
  public void appendMT10MSegBigCacheBackwardTest() throws Exception {
    final int iterations = 240;
    for (int n = 0; n < iterations; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 10 * 1024 * 1024, 1000,
          true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000);

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

      OCASDiskWriteAheadLog loadedWAL = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 10 * 1024 * 1024,
          1000, true, Locale.US, 3 * 1024 * 1024 * 1024L, -1, 1000);

      System.out.println("Assert WAL content 2");
      assertMTWALInsertion(loadedWAL, addedRecords);

      Assert.assertEquals(loadedWAL.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(loadedWAL.end(), new OLogSequenceNumber(addedRecords.lastKey().getSegment() + 1, OCASWALPage.RECORDS_OFFSET));

      loadedWAL.close();

      System.out.printf("%d iteration out of %d is passed\n", n, iterations);
    }
  }

  @Test
  public void appendMT256MSegSmallCacheTest() throws Exception {
    final int iterations = 240;
    for (int n = 0; n < iterations; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 256 * 1024 * 1024, 1000,
          true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000);

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

      OCASDiskWriteAheadLog loadedWAL = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 10 * 1024 * 1024,
          1000, true, Locale.US, 3 * 1024 * 1024 * 1024L, -1, 1000);

      System.out.println("Assert WAL content 2");
      assertMTWALInsertion(loadedWAL, addedRecords);

      Assert.assertEquals(loadedWAL.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(loadedWAL.end(), new OLogSequenceNumber(addedRecords.lastKey().getSegment() + 1, OCASWALPage.RECORDS_OFFSET));

      loadedWAL.close();

      System.out.printf("%d iteration out of %d is passed\n", n, iterations);
    }
  }

  @Test
  public void appendMT256MSegSmallCacheBackwardTest() throws Exception {
    final int iterations = 240;
    for (int n = 0; n < iterations; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 256 * 1024 * 1024, 1000,
          true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000);

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

      OCASDiskWriteAheadLog loadedWAL = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, 10 * 1024 * 1024,
          1000, true, Locale.US, 3 * 1024 * 1024 * 1024L, -1, 1000);

      System.out.println("Assert WAL content 2");
      assertMTWALInsertion(loadedWAL, addedRecords);

      Assert.assertEquals(loadedWAL.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(loadedWAL.end(), new OLogSequenceNumber(addedRecords.lastKey().getSegment() + 1, OCASWALPage.RECORDS_OFFSET));

      loadedWAL.close();

      System.out.printf("%d iteration out of %d is passed\n", n, iterations);
    }
  }

  @Test
  public void appendMT256MSegBigCacheTest() throws Exception {
    final int iterations = 240;
    for (int n = 0; n < iterations; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 256 * 1024 * 1024,
          1000, true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000);

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

      OCASDiskWriteAheadLog loadedWAL = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 10 * 1024 * 1024,
          1000, true, Locale.US, 3 * 1024 * 1024 * 1024L, -1, 1000);

      System.out.println("Assert WAL content 2");
      assertMTWALInsertion(loadedWAL, addedRecords);

      Assert.assertEquals(loadedWAL.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(loadedWAL.end(), new OLogSequenceNumber(addedRecords.lastKey().getSegment() + 1, OCASWALPage.RECORDS_OFFSET));

      loadedWAL.close();

      System.out.printf("%d iteration out of %d is passed\n", n, iterations);
    }
  }

  @Test
  public void appendMT256MSegBigCacheBackwardTest() throws Exception {
    final int iterations = 240;
    for (int n = 0; n < iterations; n++) {
      OFileUtils.deleteRecursively(testDirectory.toFile());

      OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 256 * 1024 * 1024,
          1000, true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000);

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

      OCASDiskWriteAheadLog loadedWAL = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 48_000, 10 * 1024 * 1024,
          1000, true, Locale.US, 3 * 1024 * 1024 * 1024L, -1, 1000);

      System.out.println("Assert WAL content 2");
      assertMTWALInsertion(loadedWAL, addedRecords);

      Assert.assertEquals(loadedWAL.begin(), new OLogSequenceNumber(1, OCASWALPage.RECORDS_OFFSET));
      Assert.assertEquals(loadedWAL.end(), new OLogSequenceNumber(addedRecords.lastKey().getSegment() + 1, OCASWALPage.RECORDS_OFFSET));

      loadedWAL.close();

      System.out.printf("%d iteration out of %d is passed\n", n, iterations);
    }
  }

  @Test
  public void writeBenchmarkTest() throws Exception {
    OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 49_152, 256 * 1024 * 1024, 250,
        true, Locale.US, 10 * 1024 * 1024 * 1024L, -1, 1000);

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

    List<Future<Long>> futures = new ArrayList<>();

    for (int i = 0; i < 8; i++) {
      futures.add(executorService.submit(new RawAdder(wal, walIsFull)));
    }

    long count = 0;

    long start = System.nanoTime();
    for (Future<Long> future : futures) {
      long res = future.get();
      count += res;
    }
    long end = System.nanoTime();
    final long time = end - start;

    System.out.printf("Records written %d, total time %d sec, speed %d rec/s, %d mb/s \n", count, time / 1_000_000_000,
        count * 1_000_000_000 / time, 450L * (count * 1_000_000_000 / time) / 1024 / 1024);
  }

  private void assertMTWALInsertion(OCASDiskWriteAheadLog wal, SortedMap<OLogSequenceNumber, TestRecord> addedRecords)
      throws IOException {
    final Iterator<TestRecord> recordsIterator = addedRecords.values().iterator();

    TestRecord record = recordsIterator.next();
    List<OWriteableWALRecord> records = wal.read(record.getLsn(), 100);
    Iterator<OWriteableWALRecord> readIterator = records.iterator();
    TestRecord readRecord = (TestRecord) readIterator.next();

    Assert.assertEquals(record.getLsn(), readRecord.getLsn());
    Assert.assertArrayEquals(record.data, readRecord.data);

    OLogSequenceNumber lastLSN = record.getLsn();
    while (readIterator.hasNext()) {
      readRecord = (TestRecord) readIterator.next();
      record = recordsIterator.next();

      Assert.assertEquals(record.getLsn(), readRecord.getLsn());
      Assert.assertArrayEquals(record.data, readRecord.data);
      lastLSN = record.getLsn();
    }

    while (recordsIterator.hasNext()) {
      records = wal.next(lastLSN, 100);
      Assert.assertTrue(!records.isEmpty());

      readIterator = records.iterator();
      while (readIterator.hasNext() && recordsIterator.hasNext()) {
        readRecord = (TestRecord) readIterator.next();
        record = recordsIterator.next();

        Assert.assertEquals(record.getLsn(), readRecord.getLsn());
        Assert.assertArrayEquals(record.data, readRecord.data);
        lastLSN = record.getLsn();
      }
    }

    Assert.assertTrue(!recordsIterator.hasNext());
  }

  private static final class RawAdder implements Callable<Long> {
    private final OCASDiskWriteAheadLog writeAheadLog;
    private final AtomicBoolean         walIsFull;

    public RawAdder(OCASDiskWriteAheadLog writeAheadLog, AtomicBoolean walIsFull) {
      this.writeAheadLog = writeAheadLog;
      this.walIsFull = walIsFull;
    }

    @Override
    public Long call() throws Exception {
      long counter = 0;
      ThreadLocalRandom random = ThreadLocalRandom.current();
      final byte[] data = new byte[450];
      random.nextBytes(data);

      while (!walIsFull.get()) {
        final TestRecord testRecord = new TestRecord(data);
        writeAheadLog.log(testRecord);
        counter++;
      }

      return counter;
    }
  }

  private static final class SegmentAdder implements Callable<Void> {
    private final long                  segment;
    private final OCASDiskWriteAheadLog wal;

    public SegmentAdder(long segment, OCASDiskWriteAheadLog wal) {
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

    public TestRecord() {
    }

    public TestRecord(byte[] data) {
      this.data = data;
    }

    public TestRecord(Random random, int maxSize, int minSize) {
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

  }

  public static final class RecordsAdder implements Callable<List<TestRecord>> {
    private final OCASDiskWriteAheadLog wal;
    private final AtomicBoolean         walIsFull;
    private final List<TestRecord>      addedRecords = new ArrayList<>();

    public RecordsAdder(OCASDiskWriteAheadLog wal, AtomicBoolean walIsFull) {
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
            final TestRecord record = new TestRecord(random, 4 * OCASWALPage.PAGE_SIZE, 1);
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
              continue;
            } else if (compare == 0) {
              Assert.assertArrayEquals(record.data, ((TestRecord) walRecord).data);
              break;
            } else {
              Assert.fail();
            }

          } else {
            Assert.assertNotNull(lastLSN);
            readRecords = wal.next(lastLSN, 10);
            readIterator = readRecords.iterator();

            Assert.assertTrue(readIterator.hasNext());
          }
        }

      }
    }
  }

  public static final class RecordsAdderBackwardIteration implements Callable<List<TestRecord>> {
    private final OCASDiskWriteAheadLog wal;
    private final AtomicBoolean         walIsFull;
    private final List<TestRecord>      addedRecords = new ArrayList<>();

    public RecordsAdderBackwardIteration(OCASDiskWriteAheadLog wal, AtomicBoolean walIsFull) {
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
            final TestRecord record = new TestRecord(random, 4 * OCASWALPage.PAGE_SIZE, 1);
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
      final int startIndex = addedRecords.size() - checkInterval;

      List<TestRecord> records = addedRecords.subList(startIndex, startIndex + checkInterval);
      OLogSequenceNumber lsn = records.get(0).getLsn();

      List<OWriteableWALRecord> readRecords = wal.read(lsn, 10);
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
              continue;
            } else if (compare == 0) {
              Assert.assertArrayEquals(record.data, ((TestRecord) walRecord).data);
              break;
            } else {
              Assert.fail();
            }

          } else {
            Assert.assertNotNull(lastLSN);
            readRecords = wal.next(lastLSN, 10);
            readIterator = readRecords.iterator();

            Assert.assertTrue(readIterator.hasNext());
          }
        }

      }
    }
  }
}
