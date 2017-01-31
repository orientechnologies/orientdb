package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import org.junit.*;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 16.05.13
 */
public class WriteAheadLogConcurrencyTest {
  private ODiskWriteAheadLog writeAheadLog;
  private File               testDir;
  private NavigableMap<OLogSequenceNumber, WriteAheadLogTest.TestRecord> recordConcurrentMap = new ConcurrentSkipListMap<OLogSequenceNumber, WriteAheadLogTest.TestRecord>();
  private ExecutorService writerExecutor;
  private AtomicReference<OLogSequenceNumber> lastCheckpoint = new AtomicReference<OLogSequenceNumber>();

  @Before
  public void beforeClass() throws Exception {
    OWALRecordsFactory.INSTANCE.registerNewRecord((byte) 128, WriteAheadLogTest.TestRecord.class);

    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null || buildDirectory.isEmpty())
      buildDirectory = ".";

    testDir = new File(buildDirectory, "WriteAheadLogConcurrencyTest");
    if (!testDir.exists())
      testDir.mkdir();

    OLocalPaginatedStorage localPaginatedStorage = mock(OLocalPaginatedStorage.class);
    when(localPaginatedStorage.getStoragePath()).thenReturn(Paths.get(testDir.getAbsolutePath()));
    when(localPaginatedStorage.getName()).thenReturn("WriteAheadLogConcurrencyTest");

    writeAheadLog = new ODiskWriteAheadLog(200, 500, OWALPage.PAGE_SIZE * 800, null, false, localPaginatedStorage,
        16 * OWALPage.PAGE_SIZE, 120);

    writerExecutor = Executors.newCachedThreadPool();
  }

  @Test
  @Ignore
  public void testConcurrentWrites() throws Exception {
    CountDownLatch startLatch = new CountDownLatch(1);

    List<Future> futures = new ArrayList<Future>();
    Random random = new Random();
    List<Long> seeds = new ArrayList<Long>();
    for (int i = 0; i < 8; i++)
      seeds.add(random.nextLong());

    System.out.println("Seeds");
    for (long seed : seeds)
      System.out.println(seed);

    for (int i = 0; i < 8; i++)
      futures.add(writerExecutor
          .submit(new ConcurrentWriter(seeds.get(i), startLatch, writeAheadLog, recordConcurrentMap, lastCheckpoint)));

    startLatch.countDown();

    for (Future future : futures)
      future.get();

    OLogSequenceNumber lsn = writeAheadLog.begin();
    int recordsCount = 0;
    while (lsn != null) {
      WriteAheadLogTest.TestRecord testRecord = (WriteAheadLogTest.TestRecord) writeAheadLog.read(lsn);
      Assert.assertEquals(testRecord, recordConcurrentMap.get(lsn));
      lsn = writeAheadLog.next(lsn);
      recordsCount++;
    }

    Assert.assertEquals(recordsCount, recordConcurrentMap.size());
    Assert.assertEquals(lastCheckpoint.get(), writeAheadLog.getLastCheckpoint());
  }

  @After
  public void afterClass() throws Exception {
    writeAheadLog.delete();

    if (testDir.exists())
      testDir.delete();
  }

  private static final class ConcurrentWriter implements Callable<Void> {
    private final CountDownLatch                                                 startLatch;
    private final ODiskWriteAheadLog                                             writeAheadLog;
    private final NavigableMap<OLogSequenceNumber, WriteAheadLogTest.TestRecord> recordConcurrentMap;
    private final Random                                                         random;
    private final AtomicReference<OLogSequenceNumber>                            lastCheckpoint;

    private ConcurrentWriter(long seed, CountDownLatch startLatch, ODiskWriteAheadLog writeAheadLog,
        NavigableMap<OLogSequenceNumber, WriteAheadLogTest.TestRecord> recordConcurrentMap,
        AtomicReference<OLogSequenceNumber> lastCheckpoint) {
      this.lastCheckpoint = lastCheckpoint;
      random = new Random(seed);
      this.startLatch = startLatch;
      this.writeAheadLog = writeAheadLog;
      this.recordConcurrentMap = recordConcurrentMap;
    }

    @Override
    public Void call() throws Exception {
      startLatch.await();

      try {
        while (writeAheadLog.size() < 3072L * 1024 * 1024) {
          int recordSize = random.nextInt(OWALPage.PAGE_SIZE / 2 - 128) + 128;
          WriteAheadLogTest.TestRecord testRecord = new WriteAheadLogTest.TestRecord(recordSize, random.nextBoolean());

          OLogSequenceNumber lsn = writeAheadLog.log(testRecord);
          if (testRecord.isUpdateMasterRecord()) {
            OLogSequenceNumber checkpoint = lastCheckpoint.get();
            while (checkpoint == null || checkpoint.compareTo(testRecord.getLsn()) < 0) {
              if (lastCheckpoint.compareAndSet(checkpoint, testRecord.getLsn()))
                break;

              checkpoint = lastCheckpoint.get();
            }
          }

          Assert.assertNull(recordConcurrentMap.put(lsn, testRecord));
        }

        return null;
      } catch (Exception e) {
        e.printStackTrace();
        throw e;
      }
    }
  }

}
