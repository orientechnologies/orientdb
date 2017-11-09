package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.multithreading;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;
import com.orientechnologies.orient.core.storage.impl.local.statistic.OPerformanceStatisticManager;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Test(enabled = false)
public class OWriteAheadLogCutTillTest {
  private static final int ONE_KB       = 1024;
  private static final int SEGMENT_SIZE = OWALPage.PAGE_SIZE * 2;
  private ODiskWriteAheadLog writeAheadLog;
  private File               testDir;

  @BeforeClass
  public void beforeClass() {
    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null || buildDirectory.isEmpty())
      buildDirectory = ".";

    testDir = new File(buildDirectory, "writeAheadLogTest");
    OFileUtils.deleteRecursively(testDir);
    Assert.assertTrue(testDir.mkdir());

    OWALRecordsFactory.INSTANCE.registerNewRecord((byte) 128, TestRecord.class);
  }

  @BeforeMethod
  public void beforeMethod() throws Exception {
    writeAheadLog = createWAL();
  }

  private ODiskWriteAheadLog createWAL() throws IOException {
    return createWAL(100, SEGMENT_SIZE);
  }

  private ODiskWriteAheadLog createWAL(int maxPagesCacheSize, int maxSegmentSize) throws IOException {
    OLocalPaginatedStorage paginatedStorage = mock(OLocalPaginatedStorage.class);
    when(paginatedStorage.getName()).thenReturn("OWriteAheadLogCutTillTest");
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
    OFileUtils.deleteRecursively(testDir);
  }

  public void cutTillTest() throws Exception {
    final ExecutorService executorService = Executors.newCachedThreadPool();
    try {
      final List<Future<Void>> futures = new ArrayList<Future<Void>>();

      final AtomicBoolean stop = new AtomicBoolean();
      futures.add(executorService.submit(new Writer(writeAheadLog, stop)));
      futures.add(executorService.submit(new Writer(writeAheadLog, stop)));

      Thread.sleep(30000);

      futures.add(executorService.submit(new Reader(writeAheadLog, stop)));
      futures.add(executorService.submit(new Reader(writeAheadLog, stop)));
      futures.add(executorService.submit(new Reader(writeAheadLog, stop)));

      futures.add(executorService.submit(new Cutter(writeAheadLog, stop)));
      futures.add(executorService.submit(new Cutter(writeAheadLog, stop)));
      futures.add(executorService.submit(new Cutter(writeAheadLog, stop)));

      Thread.sleep(300000);

      stop.set(true);

      for (Future<Void> future : futures) {
        future.get();
      }
    } finally {
      executorService.shutdown();
    }
  }

  public static final class Reader implements Callable<Void> {
    private final ODiskWriteAheadLog writeAheadLog;
    private final AtomicBoolean      stop;

    private final Random random = new Random();

    public Reader(ODiskWriteAheadLog writeAheadLog, AtomicBoolean stop) {
      this.writeAheadLog = writeAheadLog;
      this.stop = stop;
    }

    @Override
    public Void call() throws Exception {
      try {
        while (!stop.get()) {
          OLogSequenceNumber startLSN = writeAheadLog.begin();
          if (startLSN == null)
            continue;

          final OLogSequenceNumber freezeLSN = startLSN;
          writeAheadLog.addCutTillLimit(freezeLSN);
          try {
            startLSN = writeAheadLog.begin();
            if (startLSN == null)
              continue;

            OLogSequenceNumber lsn = startLSN;
            while (lsn != null) {
              final OWALRecord walRecord = writeAheadLog.read(lsn);
              lsn = writeAheadLog.next(lsn);
            }
          } finally {
            writeAheadLog.removeCutTillLimit(freezeLSN);
          }

          Thread.sleep(random.nextInt(5000) + 100);
        }
      } catch (Exception e) {
        OLogManager.instance().errorNoDb(this, "Error during WAL reading", e);
        throw e;
      }

      return null;
    }
  }

  private static final class Writer implements Callable<Void> {
    private final ODiskWriteAheadLog writeAheadLog;
    private final AtomicBoolean      stop;

    private final Random random = new Random();

    public Writer(ODiskWriteAheadLog writeAheadLog, AtomicBoolean stop) {
      this.writeAheadLog = writeAheadLog;
      this.stop = stop;
    }

    @Override
    public Void call() throws Exception {
      try {
        while (!stop.get()) {
          if (writeAheadLog.size() > 256 * 1024 * 1024) {
            Thread.yield();
            continue;
          }

          final TestRecord walRecord = new TestRecord(random.nextInt(1024) + 64);
          writeAheadLog.log(walRecord);
        }
      } catch (Exception e) {
        OLogManager.instance().errorNoDb(this, "Error during writing of data into the WAL", e);
        throw e;
      }
      return null;
    }
  }

  private static final class Cutter implements Callable<Void> {
    private final ODiskWriteAheadLog writeAheadLog;
    private final AtomicBoolean      stop;

    private final Random random = new Random();

    public Cutter(ODiskWriteAheadLog writeAheadLog, AtomicBoolean stop) {
      this.writeAheadLog = writeAheadLog;
      this.stop = stop;
    }

    @Override
    public Void call() throws Exception {
      try {
        int cutCounter = 0;

        while (!stop.get()) {
          final OLogSequenceNumber begin = writeAheadLog.begin();
          if (begin == null)
            continue;

          final int segment = random.nextInt(4) + 1;
          final int position = random.nextInt(16) + 1;

          if (writeAheadLog.cutTill(new OLogSequenceNumber(begin.getSegment() + segment, begin.getPosition() + position))) {
            cutCounter++;

            if (cutCounter > 0 && cutCounter % 100 == 0) {
              System.out.printf("Cut was done %d times\n", cutCounter);
            }
          }
        }

        System.out.printf("Cut was done %d times\n", cutCounter);
      } catch (Exception e) {
        OLogManager.instance().errorNoDb(this, "Error during cutting of WAL", e);
      }

      return null;
    }
  }

  public static final class TestRecord extends OAbstractWALRecord {
    private byte[] data;

    /**
     * Used for log record deserialization.
     */
    @SuppressWarnings("unused")
    public TestRecord() {
    }

    public TestRecord(int size) {
      Random random = new Random();
      data = new byte[size];
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
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      if (!super.equals(o))
        return false;

      TestRecord that = (TestRecord) o;

      return Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + Arrays.hashCode(data);
      return result;
    }
  }

}
