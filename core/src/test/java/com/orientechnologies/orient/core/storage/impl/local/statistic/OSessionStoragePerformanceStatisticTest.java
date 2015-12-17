package com.orientechnologies.orient.core.storage.impl.local.statistic;

import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Test
public class OSessionStoragePerformanceStatisticTest {
  public void testReadFromCache() {
    OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic =
        new OSessionStoragePerformanceStatistic(1024, new OSessionStoragePerformanceStatistic.NanoTimer() {
          private long counter = 0;

          @Override
          public long getNano() {
            return counter += 100;
          }
        });


    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesReadFromCache(), 0);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromCacheInMB(), -1);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromCacheInMB(), -1);

    sessionStoragePerformanceStatistic.startCommitTimer();
    for (int i = 0; i < 100; i++) {
      sessionStoragePerformanceStatistic.startPageReadFromCacheTimer();
      sessionStoragePerformanceStatistic.stopPageReadFromCacheTimer();
    }
    sessionStoragePerformanceStatistic.stopCommitTimer();

    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesReadFromCache(), 100);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromCacheInPages(), 10000000);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromCacheInMB(), 10000000 / 1024);

    final ODocument doc = sessionStoragePerformanceStatistic.toDocument();

    Assert.assertEquals(doc.field("amountOfPagesReadFromCache"), 100L);
    Assert.assertEquals(doc.field("readSpeedFromCacheInPages"), 10000000L);
    Assert.assertEquals(doc.field("readSpeedFromCacheInMB"), 10000000L / 1024);
  }

  public void testReadFromFile() {
    OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic =
        new OSessionStoragePerformanceStatistic(1024, new OSessionStoragePerformanceStatistic.NanoTimer() {
          private long counter = 0;

          @Override
          public long getNano() {
            return counter += 100;
          }
        });


    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesReadFromFile(), 0);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromFileInPages(), -1);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromFileInMB(), -1);

    sessionStoragePerformanceStatistic.startCommitTimer();
    for (int i = 0; i < 100; i++) {
      sessionStoragePerformanceStatistic.startPageReadFromFileTimer();
      sessionStoragePerformanceStatistic.stopPageReadFromFileTimer(10);
    }
    sessionStoragePerformanceStatistic.stopCommitTimer();

    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesReadFromFile(), 1000);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromFileInPages(), 100000000);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromFileInMB(), 100000000 / 1024);

    final ODocument doc = sessionStoragePerformanceStatistic.toDocument();

    Assert.assertEquals(doc.field("amountOfPagesReadFromFile"), 1000L);
    Assert.assertEquals(doc.field("readSpeedFromFileInPages"), 100000000L);
    Assert.assertEquals(doc.field("readSpeedFromFileInMB"), 100000000L / 1024);

  }

  public void testWriteInCache() {
    OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic =
        new OSessionStoragePerformanceStatistic(1024, new OSessionStoragePerformanceStatistic.NanoTimer() {
          private long counter = 0;

          @Override
          public long getNano() {
            return counter += 100;
          }
        });


    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesWrittenInCache(), 0);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getWriteSpeedInCacheInPages(), -1);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getWriteSpeedInCacheInMB(), -1);

    sessionStoragePerformanceStatistic.startCommitTimer();
    for (int i = 0; i < 100; i++) {
      sessionStoragePerformanceStatistic.startPageWriteInCacheTimer();
      sessionStoragePerformanceStatistic.stopPageWriteInCacheTimer();
    }
    sessionStoragePerformanceStatistic.stopCommitTimer();

    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesWrittenInCache(), 100);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getWriteSpeedInCacheInPages(), 10000000);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getWriteSpeedInCacheInMB(), 10000000 / 1024);

    ODocument doc = sessionStoragePerformanceStatistic.toDocument();

    Assert.assertEquals(doc.field("amountOfPagesWrittenInCache"), 100L);
    Assert.assertEquals(doc.field("writeSpeedInCacheInPages"), 10000000L);
    Assert.assertEquals(doc.field("writeSpeedInCacheInMB"), 10000000 / 1024L);
  }

  public void testCommitCount() {
    OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic =
        new OSessionStoragePerformanceStatistic(1024, new OSessionStoragePerformanceStatistic.NanoTimer() {
          private long counter = 0;

          @Override
          public long getNano() {
            return counter += 100;
          }
        });

    Assert.assertEquals(sessionStoragePerformanceStatistic.getCommitTimeAvg(), -1);

    sessionStoragePerformanceStatistic.startPageReadFromCacheTimer();

    for (int i = 0; i < 100; i++) {
      sessionStoragePerformanceStatistic.startCommitTimer();
      sessionStoragePerformanceStatistic.stopCommitTimer();
    }

    sessionStoragePerformanceStatistic.stopPageReadFromCacheTimer();

    Assert.assertEquals(sessionStoragePerformanceStatistic.getCommitTimeAvg(), 100);

    final ODocument doc = sessionStoragePerformanceStatistic.toDocument();

    Assert.assertEquals(doc.field("commitTimeAvg"), 100L);
  }

  public void testCacheHit() {
    OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic =
        new OSessionStoragePerformanceStatistic(1024, new OSessionStoragePerformanceStatistic.NanoTimer() {
          private long counter = 0;

          @Override
          public long getNano() {
            return counter += 100;
          }
        });


    Assert.assertEquals(sessionStoragePerformanceStatistic.getCacheHits(), -1);

    for (int i = 0; i < 100; i++) {
      sessionStoragePerformanceStatistic.incrementPageAccessOnCacheLevel();
      if (i % 2 == 0)
        sessionStoragePerformanceStatistic.incrementCacheHit();
    }

    Assert.assertEquals(sessionStoragePerformanceStatistic.getCacheHits(), 50);

    final ODocument doc = sessionStoragePerformanceStatistic.toDocument();

    Assert.assertEquals(doc.field("cacheHits"), 50);
  }

  public void testSession() throws Exception {
    Assert.assertNull(OSessionStoragePerformanceStatistic.getStatisticInstance());
    OSessionStoragePerformanceStatistic.initThreadLocalInstance(1024);

    final OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic =
        OSessionStoragePerformanceStatistic.getStatisticInstance();
    Assert.assertNotNull(sessionStoragePerformanceStatistic);

    ExecutorService service = Executors.newCachedThreadPool();
    Future<Void> future = service.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        Assert.assertNull(OSessionStoragePerformanceStatistic.getStatisticInstance());
        return null;
      }
    });

    future.get();

    service.shutdown();

    Assert.assertSame(OSessionStoragePerformanceStatistic.clearThreadLocalInstance(), sessionStoragePerformanceStatistic);
    Assert.assertNull(OSessionStoragePerformanceStatistic.getStatisticInstance());
    Assert.assertNull(OSessionStoragePerformanceStatistic.clearThreadLocalInstance());
  }
}
