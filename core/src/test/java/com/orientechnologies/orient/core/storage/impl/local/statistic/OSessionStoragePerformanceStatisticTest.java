package com.orientechnologies.orient.core.storage.impl.local.statistic;

import com.orientechnologies.common.types.OModifiableInteger;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Test
public class OSessionStoragePerformanceStatisticTest {
  public void testReadFromCache() {
    final OModifiableInteger increment = new OModifiableInteger();

    OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = new OSessionStoragePerformanceStatistic(1024,
        new OSessionStoragePerformanceStatistic.NanoTimer() {
          private long counter = 0;

          @Override
          public long getNano() {
            return counter += increment.getValue();
          }
        });

    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesReadFromCache(), 0);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromCacheInMB(), -1);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromCacheInMB(), -1);

    sessionStoragePerformanceStatistic.startCommitTimer();
    increment.setValue(50);

    sessionStoragePerformanceStatistic.setCurrentComponent("c1po");
    for (int i = 0; i < 50; i++) {
      sessionStoragePerformanceStatistic.startPageReadFromCacheTimer();
      sessionStoragePerformanceStatistic.stopPageReadFromCacheTimer();
    }
    sessionStoragePerformanceStatistic.clearCurrentComponent();

    sessionStoragePerformanceStatistic.setCurrentComponent("c2po");
    increment.setValue(150);
    for (int i = 0; i < 50; i++) {
      sessionStoragePerformanceStatistic.startPageReadFromCacheTimer();
      sessionStoragePerformanceStatistic.stopPageReadFromCacheTimer();
    }
    sessionStoragePerformanceStatistic.clearCurrentComponent();

    increment.setValue(100);
    for (int i = 0; i < 50; i++) {
      sessionStoragePerformanceStatistic.startPageReadFromCacheTimer();
      sessionStoragePerformanceStatistic.stopPageReadFromCacheTimer();
    }

    sessionStoragePerformanceStatistic.stopCommitTimer();

    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesReadFromCache(), 150);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesReadFromCache(null), 150);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesReadFromCache("c1po"), 50);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesReadFromCache("c2po"), 50);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesReadFromCache("c3po"), -1);

    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromCacheInPages(), 10000000);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromCacheInPages(null), 10000000);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromCacheInPages("c1po"), 20000000);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromCacheInPages("c2po"), 6666666);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromCacheInPages("c3po"), -1);

    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromCacheInMB(), 10000000 / 1024);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromCacheInMB(null), 10000000 / 1024);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromCacheInMB("c1po"), 20000000 / 1024);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromCacheInMB("c2po"), 6666666 / 1024);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromCacheInMB("c3po"), -1);

    final ODocument doc = sessionStoragePerformanceStatistic.toDocument();

    Assert.assertEquals(doc.field("amountOfPagesReadFromCache"), 150L);
    Assert.assertEquals(doc.field("readSpeedFromCacheInPages"), 10000000L);
    Assert.assertEquals(doc.field("readSpeedFromCacheInMB"), 10000000L / 1024);

    final ODocument docC1PO = doc.<Map<String, ODocument>>field("dataByComponent").get("c1po");

    Assert.assertEquals(docC1PO.field("amountOfPagesReadFromCache"), 50L);
    Assert.assertEquals(docC1PO.field("readSpeedFromCacheInPages"), 20000000L);
    Assert.assertEquals(docC1PO.field("readSpeedFromCacheInMB"), 20000000L / 1024);

    final ODocument docC2PO = doc.<Map<String, ODocument>>field("dataByComponent").get("c2po");

    Assert.assertEquals(docC2PO.field("amountOfPagesReadFromCache"), 50L);
    Assert.assertEquals(docC2PO.field("readSpeedFromCacheInPages"), 6666666L);
    Assert.assertEquals(docC2PO.field("readSpeedFromCacheInMB"), 6666666L / 1024);
  }

  public void testReadFromFile() {
    final OModifiableInteger increment = new OModifiableInteger();

    OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = new OSessionStoragePerformanceStatistic(1024,
        new OSessionStoragePerformanceStatistic.NanoTimer() {
          private long counter = 0;

          @Override
          public long getNano() {
            return counter += increment.getValue();
          }
        });

    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesReadFromFile(), 0);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromFileInPages(), -1);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromFileInMB(), -1);

    sessionStoragePerformanceStatistic.startCommitTimer();

    increment.setValue(50);
    sessionStoragePerformanceStatistic.setCurrentComponent("c1po");
    for (int i = 0; i < 50; i++) {
      sessionStoragePerformanceStatistic.startPageReadFromFileTimer();
      sessionStoragePerformanceStatistic.stopPageReadFromFileTimer(10);
    }
    sessionStoragePerformanceStatistic.clearCurrentComponent();

    increment.setValue(150);
    sessionStoragePerformanceStatistic.setCurrentComponent("c2po");
    for (int i = 0; i < 50; i++) {
      sessionStoragePerformanceStatistic.startPageReadFromFileTimer();
      sessionStoragePerformanceStatistic.stopPageReadFromFileTimer(10);
    }
    sessionStoragePerformanceStatistic.clearCurrentComponent();

    increment.setValue(100);
    for (int i = 0; i < 50; i++) {
      sessionStoragePerformanceStatistic.startPageReadFromFileTimer();
      sessionStoragePerformanceStatistic.stopPageReadFromFileTimer(10);
    }

    sessionStoragePerformanceStatistic.stopCommitTimer();

    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesReadFromFile(), 1500);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesReadFromFile(null), 1500);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesReadFromFile("c1po"), 500);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesReadFromFile("c2po"), 500);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesReadFromFile("c3po"), -1);

    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromFileInPages(), 100000000);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromFileInPages(null), 100000000);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromFileInPages("c1po"), 200000000);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromFileInPages("c2po"), 66666666);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromFileInPages("c3po"), -1);

    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromFileInMB(), 100000000 / 1024);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromFileInMB(null), 100000000 / 1024);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromFileInMB("c1po"), 200000000 / 1024);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromFileInMB("c2po"), 66666666 / 1024);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromFileInMB("c3po"), -1);

    final ODocument doc = sessionStoragePerformanceStatistic.toDocument();

    Assert.assertEquals(doc.field("amountOfPagesReadFromFile"), 1500L);
    Assert.assertEquals(doc.field("readSpeedFromFileInPages"), 100000000L);
    Assert.assertEquals(doc.field("readSpeedFromFileInMB"), 100000000L / 1024);

    final ODocument docC1PO = doc.<Map<String, ODocument>>field("dataByComponent").get("c1po");

    Assert.assertEquals(docC1PO.field("amountOfPagesReadFromFile"), 500L);
    Assert.assertEquals(docC1PO.field("readSpeedFromFileInPages"), 200000000L);
    Assert.assertEquals(docC1PO.field("readSpeedFromFileInMB"), 200000000L / 1024);

    final ODocument docC2PO = doc.<Map<String, ODocument>>field("dataByComponent").get("c2po");

    Assert.assertEquals(docC2PO.field("amountOfPagesReadFromFile"), 500L);
    Assert.assertEquals(docC2PO.field("readSpeedFromFileInPages"), 66666666L);
    Assert.assertEquals(docC2PO.field("readSpeedFromFileInMB"), 66666666L / 1024);
  }

  public void testWriteInCache() {
    final OModifiableInteger increment = new OModifiableInteger();

    OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = new OSessionStoragePerformanceStatistic(1024,
        new OSessionStoragePerformanceStatistic.NanoTimer() {
          private long counter = 0;

          @Override
          public long getNano() {
            return counter += increment.getValue();
          }
        });

    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesWrittenInCache(), 0);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getWriteSpeedInCacheInPages(), -1);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getWriteSpeedInCacheInMB(), -1);

    sessionStoragePerformanceStatistic.startCommitTimer();
    increment.setValue(50);
    sessionStoragePerformanceStatistic.setCurrentComponent("c1po");
    for (int i = 0; i < 50; i++) {
      sessionStoragePerformanceStatistic.startPageWriteInCacheTimer();
      sessionStoragePerformanceStatistic.stopPageWriteInCacheTimer();
    }
    sessionStoragePerformanceStatistic.clearCurrentComponent();

    increment.setValue(150);
    sessionStoragePerformanceStatistic.setCurrentComponent("c2po");
    for (int i = 0; i < 50; i++) {
      sessionStoragePerformanceStatistic.startPageWriteInCacheTimer();
      sessionStoragePerformanceStatistic.stopPageWriteInCacheTimer();
    }
    sessionStoragePerformanceStatistic.clearCurrentComponent();

    increment.setValue(100);
    for (int i = 0; i < 50; i++) {
      sessionStoragePerformanceStatistic.startPageWriteInCacheTimer();
      sessionStoragePerformanceStatistic.stopPageWriteInCacheTimer();
    }

    sessionStoragePerformanceStatistic.stopCommitTimer();

    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesWrittenInCache(), 150);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesWrittenInCache(null), 150);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesWrittenInCache("c1po"), 50);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesWrittenInCache("c2po"), 50);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesWrittenInCache("c3po"), -1);

    Assert.assertEquals(sessionStoragePerformanceStatistic.getWriteSpeedInCacheInPages(), 10000000);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getWriteSpeedInCacheInPages(null), 10000000);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getWriteSpeedInCacheInPages("c1po"), 20000000);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getWriteSpeedInCacheInPages("c2po"), 6666666);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getWriteSpeedInCacheInPages("c3po"), -1);

    Assert.assertEquals(sessionStoragePerformanceStatistic.getWriteSpeedInCacheInMB(), 10000000 / 1024);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getWriteSpeedInCacheInMB(null), 10000000 / 1024);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getWriteSpeedInCacheInMB("c1po"), 20000000 / 1024);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getWriteSpeedInCacheInMB("c2po"), 6666666 / 1024);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getWriteSpeedInCacheInMB("c3po"), -1);

    ODocument doc = sessionStoragePerformanceStatistic.toDocument();

    Assert.assertEquals(doc.field("amountOfPagesWrittenInCache"), 150L);
    Assert.assertEquals(doc.field("writeSpeedInCacheInPages"), 10000000L);
    Assert.assertEquals(doc.field("writeSpeedInCacheInMB"), 10000000 / 1024L);

    final ODocument docC1PO = doc.<Map<String, ODocument>>field("dataByComponent").get("c1po");

    Assert.assertEquals(docC1PO.field("amountOfPagesWrittenInCache"), 50L);
    Assert.assertEquals(docC1PO.field("writeSpeedInCacheInPages"), 20000000L);
    Assert.assertEquals(docC1PO.field("writeSpeedInCacheInMB"), 20000000 / 1024L);

    final ODocument docC2PO = doc.<Map<String, ODocument>>field("dataByComponent").get("c2po");

    Assert.assertEquals(docC2PO.field("amountOfPagesWrittenInCache"), 50L);
    Assert.assertEquals(docC2PO.field("writeSpeedInCacheInPages"), 6666666L);
    Assert.assertEquals(docC2PO.field("writeSpeedInCacheInMB"), 6666666 / 1024L);
  }

  public void testCommitCount() {
    OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = new OSessionStoragePerformanceStatistic(1024,
        new OSessionStoragePerformanceStatistic.NanoTimer() {
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
    OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = new OSessionStoragePerformanceStatistic(1024,
        new OSessionStoragePerformanceStatistic.NanoTimer() {
          private long counter = 0;

          @Override
          public long getNano() {
            return counter += 100;
          }
        });

    Assert.assertEquals(sessionStoragePerformanceStatistic.getCacheHits(), -1);

    sessionStoragePerformanceStatistic.setCurrentComponent("c1po");
    for (int i = 0; i < 50; i++) {
      sessionStoragePerformanceStatistic.incrementPageAccessOnCacheLevel(i % 2 == 0);
    }
    sessionStoragePerformanceStatistic.clearCurrentComponent();

    sessionStoragePerformanceStatistic.setCurrentComponent("c2po");
    for (int i = 0; i < 50; i++) {
      sessionStoragePerformanceStatistic.incrementPageAccessOnCacheLevel(true);
    }
    sessionStoragePerformanceStatistic.clearCurrentComponent();

    for (int i = 0; i < 50; i++) {
      sessionStoragePerformanceStatistic.incrementPageAccessOnCacheLevel(false);
    }

    Assert.assertEquals(sessionStoragePerformanceStatistic.getCacheHits(), 50);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getCacheHits(null), 50);

    Assert.assertEquals(sessionStoragePerformanceStatistic.getCacheHits("c1po"), 50);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getCacheHits("c2po"), 100);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getCacheHits("c3po"), -1);

    final ODocument doc = sessionStoragePerformanceStatistic.toDocument();

    Assert.assertEquals(doc.field("cacheHits"), 50);

    final ODocument docC1PO = doc.<Map<String, ODocument>>field("dataByComponent").get("c1po");
    Assert.assertEquals(docC1PO.field("cacheHits"), 50);

    final ODocument docC2PO = doc.<Map<String, ODocument>>field("dataByComponent").get("c2po");
    Assert.assertEquals(docC2PO.field("cacheHits"), 100);
  }

  public void testSession() throws Exception {
    Assert.assertNull(OSessionStoragePerformanceStatistic.getStatisticInstance());
    OSessionStoragePerformanceStatistic.initThreadLocalInstance(1024);

    final OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = OSessionStoragePerformanceStatistic
        .getStatisticInstance();
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
