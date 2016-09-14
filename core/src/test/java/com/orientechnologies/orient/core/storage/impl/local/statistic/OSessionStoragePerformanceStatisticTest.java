package com.orientechnologies.orient.core.storage.impl.local.statistic;

import com.orientechnologies.common.types.OModifiableInteger;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class OSessionStoragePerformanceStatisticTest {

  @Test
  public void testReadFromCache() {
    final OModifiableInteger increment = new OModifiableInteger();

    OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = new OSessionStoragePerformanceStatistic(100,
        new OSessionStoragePerformanceStatistic.NanoTimer() {
          private long counter = 0;

          @Override
          public long getNano() {
            return counter += increment.getValue();
          }
        }, -1);

    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesReadFromCache(), 0);

    sessionStoragePerformanceStatistic.startCommitTimer();

    for (int i = 0; i < 50; i++) {
      increment.setValue(50);
      sessionStoragePerformanceStatistic.startComponentOperation("c1po", OSessionStoragePerformanceStatistic.ComponentType.GENERAL);
      sessionStoragePerformanceStatistic.startComponentOperation("c1po", OSessionStoragePerformanceStatistic.ComponentType.GENERAL);
      sessionStoragePerformanceStatistic.startPageReadFromCacheTimer();
      sessionStoragePerformanceStatistic.stopPageReadFromCacheTimer();

      increment.setValue(150);
      sessionStoragePerformanceStatistic
          .startComponentOperation("c2po", OSessionStoragePerformanceStatistic.ComponentType.GENERAL);//c2po inside of c1po
      sessionStoragePerformanceStatistic.startPageReadFromCacheTimer();
      sessionStoragePerformanceStatistic.stopPageReadFromCacheTimer();
      sessionStoragePerformanceStatistic.completeComponentOperation();//c2po

      sessionStoragePerformanceStatistic.completeComponentOperation();//c1po first
      sessionStoragePerformanceStatistic.completeComponentOperation();//c1po last
    }

    increment.setValue(100);
    for (int i = 0; i < 50; i++) {
      sessionStoragePerformanceStatistic.startPageReadFromCacheTimer();
      sessionStoragePerformanceStatistic.stopPageReadFromCacheTimer();
    }

    sessionStoragePerformanceStatistic.stopCommitTimer();

    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesPerOperation("c3po"), -1);

    sessionStoragePerformanceStatistic.startComponentOperation("c4po", OSessionStoragePerformanceStatistic.ComponentType.GENERAL);

    sessionStoragePerformanceStatistic.startComponentOperation("c3po", OSessionStoragePerformanceStatistic.ComponentType.GENERAL);
    sessionStoragePerformanceStatistic.completeComponentOperation();

    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesPerOperation(null), -1);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesPerOperation("c1po"), 2);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesPerOperation("c2po"), 1);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesPerOperation("c3po"), 0);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesPerOperation("c4po"), -1);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesPerOperation("c5po"), -1);

    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesReadFromCache(), 150);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesReadFromCache(null), 150);

    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesReadFromCache("c1po"), 100);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesReadFromCache("c2po"), 50);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesReadFromCache("c3po"), 0);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesReadFromCache("c4po"), -1);

    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromCacheInPages(), 10000000);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromCacheInPages(null), 10000000);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromCacheInPages("c1po"), 10000000);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromCacheInPages("c2po"), 6666666);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromCacheInPages("c3po"), -1);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromCacheInPages("c4po"), -1);

    final ODocument doc = sessionStoragePerformanceStatistic.toDocument();

    Assert.assertEquals(doc.field("amountOfPagesReadFromCache"), Long.valueOf(150L));
    Assert.assertEquals(doc.field("readSpeedFromCacheInPages"), Long.valueOf(10000000L));

    final ODocument docC1PO = doc.<Map<String, ODocument>>field("dataByComponent").get("c1po");

    Assert.assertEquals(docC1PO.field("amountOfPagesReadFromCache"), Long.valueOf(100L));
    Assert.assertEquals(docC1PO.field("readSpeedFromCacheInPages"), Long.valueOf(10000000L));
    Assert.assertEquals(docC1PO.field("amountOfPagesPerOperation"), Long.valueOf(2L));

    final ODocument docC2PO = doc.<Map<String, ODocument>>field("dataByComponent").get("c2po");

    Assert.assertEquals(docC2PO.field("amountOfPagesReadFromCache"), Long.valueOf(50L));
    Assert.assertEquals(docC2PO.field("readSpeedFromCacheInPages"), Long.valueOf(6666666L));
    Assert.assertEquals(docC2PO.field("amountOfPagesPerOperation"), Long.valueOf(1L));
  }

  @Test
  public void testReadFromFile() {
    final OModifiableInteger increment = new OModifiableInteger();

    OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = new OSessionStoragePerformanceStatistic(100,
        new OSessionStoragePerformanceStatistic.NanoTimer() {
          private long counter = 0;

          @Override
          public long getNano() {
            return counter += increment.getValue();
          }
        }, -1);

    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesReadFromFile(), 0);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromFileInPages(), -1);

    sessionStoragePerformanceStatistic.startCommitTimer();

    for (int i = 0; i < 50; i++) {
      increment.setValue(50);
      sessionStoragePerformanceStatistic.startComponentOperation("c1po", OSessionStoragePerformanceStatistic.ComponentType.GENERAL);
      sessionStoragePerformanceStatistic.startComponentOperation("c1po", OSessionStoragePerformanceStatistic.ComponentType.GENERAL);
      sessionStoragePerformanceStatistic.startPageReadFromFileTimer();
      sessionStoragePerformanceStatistic.stopPageReadFromFileTimer(10);

      increment.setValue(150);
      sessionStoragePerformanceStatistic.startComponentOperation("c2po", OSessionStoragePerformanceStatistic.ComponentType.GENERAL);
      sessionStoragePerformanceStatistic.startPageReadFromFileTimer();
      sessionStoragePerformanceStatistic.stopPageReadFromFileTimer(10);
      sessionStoragePerformanceStatistic.completeComponentOperation();

      sessionStoragePerformanceStatistic.completeComponentOperation();
      sessionStoragePerformanceStatistic.completeComponentOperation();
    }

    increment.setValue(100);
    for (int i = 0; i < 50; i++) {
      sessionStoragePerformanceStatistic.startPageReadFromFileTimer();
      sessionStoragePerformanceStatistic.stopPageReadFromFileTimer(10);
    }

    sessionStoragePerformanceStatistic.stopCommitTimer();

    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesReadFromFile(), 1500);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesReadFromFile(null), 1500);

    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesReadFromFile("c1po"), 1000);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesReadFromFile("c2po"), 500);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesReadFromFile("c3po"), -1);

    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromFileInPages(), 100000000);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromFileInPages(null), 100000000);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromFileInPages("c1po"), 100000000);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromFileInPages("c2po"), 66666666);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getReadSpeedFromFileInPages("c3po"), -1);

    final ODocument doc = sessionStoragePerformanceStatistic.toDocument();

    Assert.assertEquals(doc.<Long>field("amountOfPagesReadFromFile"), Long.valueOf(1500L));
    Assert.assertEquals(doc.<Long>field("readSpeedFromFileInPages"), Long.valueOf(100000000L));

    final ODocument docC1PO = doc.<Map<String, ODocument>>field("dataByComponent").get("c1po");

    Assert.assertEquals(docC1PO.<Long>field("amountOfPagesReadFromFile"), Long.valueOf(1000L));
    Assert.assertEquals(docC1PO.field("readSpeedFromFileInPages"), Long.valueOf(100000000L));

    final ODocument docC2PO = doc.<Map<String, ODocument>>field("dataByComponent").get("c2po");

    Assert.assertEquals(docC2PO.field("amountOfPagesReadFromFile"), Long.valueOf(500L));
    Assert.assertEquals(docC2PO.field("readSpeedFromFileInPages"), Long.valueOf(66666666L));
  }

  @Test
  public void testWriteInCache() {
    final OModifiableInteger increment = new OModifiableInteger();

    OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = new OSessionStoragePerformanceStatistic(100,
        new OSessionStoragePerformanceStatistic.NanoTimer() {
          private long counter = 0;

          @Override
          public long getNano() {
            return counter += increment.getValue();
          }
        }, -1);

    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesWrittenInCache(), 0);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getWriteSpeedInCacheInPages(), -1);

    sessionStoragePerformanceStatistic.startCommitTimer();

    for (int i = 0; i < 50; i++) {
      increment.setValue(50);
      sessionStoragePerformanceStatistic.startComponentOperation("c1po", OSessionStoragePerformanceStatistic.ComponentType.GENERAL);
      sessionStoragePerformanceStatistic.startComponentOperation("c1po", OSessionStoragePerformanceStatistic.ComponentType.GENERAL);
      sessionStoragePerformanceStatistic.startPageWriteInCacheTimer();
      sessionStoragePerformanceStatistic.stopPageWriteInCacheTimer();

      increment.setValue(150);
      sessionStoragePerformanceStatistic.startComponentOperation("c2po", OSessionStoragePerformanceStatistic.ComponentType.GENERAL);
      sessionStoragePerformanceStatistic.startPageWriteInCacheTimer();
      sessionStoragePerformanceStatistic.stopPageWriteInCacheTimer();
      sessionStoragePerformanceStatistic.completeComponentOperation();

      sessionStoragePerformanceStatistic.completeComponentOperation();
      sessionStoragePerformanceStatistic.completeComponentOperation();
    }

    increment.setValue(100);
    for (int i = 0; i < 50; i++) {
      sessionStoragePerformanceStatistic.startPageWriteInCacheTimer();
      sessionStoragePerformanceStatistic.stopPageWriteInCacheTimer();
    }

    sessionStoragePerformanceStatistic.stopCommitTimer();

    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesWrittenInCache(), 150);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesWrittenInCache(null), 150);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesWrittenInCache("c1po"), 100);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesWrittenInCache("c2po"), 50);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getAmountOfPagesWrittenInCache("c3po"), -1);

    Assert.assertEquals(sessionStoragePerformanceStatistic.getWriteSpeedInCacheInPages(), 10000000);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getWriteSpeedInCacheInPages(null), 10000000);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getWriteSpeedInCacheInPages("c1po"), 10000000);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getWriteSpeedInCacheInPages("c2po"), 6666666);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getWriteSpeedInCacheInPages("c3po"), -1);

    ODocument doc = sessionStoragePerformanceStatistic.toDocument();

    Assert.assertEquals(doc.field("amountOfPagesWrittenInCache"), Long.valueOf(150L));
    Assert.assertEquals(doc.field("writeSpeedInCacheInPages"), Long.valueOf(10000000L));

    final ODocument docC1PO = doc.<Map<String, ODocument>>field("dataByComponent").get("c1po");

    Assert.assertEquals(docC1PO.field("amountOfPagesWrittenInCache"), Long.valueOf(100L));
    Assert.assertEquals(docC1PO.field("writeSpeedInCacheInPages"), Long.valueOf(10000000L));

    final ODocument docC2PO = doc.<Map<String, ODocument>>field("dataByComponent").get("c2po");

    Assert.assertEquals(docC2PO.field("amountOfPagesWrittenInCache"), Long.valueOf(50L));
    Assert.assertEquals(docC2PO.field("writeSpeedInCacheInPages"), Long.valueOf(6666666L));
  }

  @Test
  public void testCommitCount() {
    OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = new OSessionStoragePerformanceStatistic(100,
        new OSessionStoragePerformanceStatistic.NanoTimer() {
          private long counter = 0;

          @Override
          public long getNano() {
            return counter += 100;
          }
        }, -1);

    Assert.assertEquals(sessionStoragePerformanceStatistic.getCommitTime(), -1);

    sessionStoragePerformanceStatistic.startPageReadFromCacheTimer();

    for (int i = 0; i < 100; i++) {
      sessionStoragePerformanceStatistic.startCommitTimer();
      sessionStoragePerformanceStatistic.stopCommitTimer();
    }

    sessionStoragePerformanceStatistic.stopPageReadFromCacheTimer();

    Assert.assertEquals(sessionStoragePerformanceStatistic.getCommitTime(), 100);

    final ODocument doc = sessionStoragePerformanceStatistic.toDocument();

    Assert.assertEquals(doc.field("commitTime"), Long.valueOf(100L));
  }

  @Test
  public void testCacheHit() {
    OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = new OSessionStoragePerformanceStatistic(100,
        new OSessionStoragePerformanceStatistic.NanoTimer() {
          private long counter = 0;

          @Override
          public long getNano() {
            return counter += 100;
          }
        }, -1);

    Assert.assertEquals(sessionStoragePerformanceStatistic.getCacheHits(), -1);

    for (int i = 0; i < 50; i++) {
      sessionStoragePerformanceStatistic.startComponentOperation("c1po", OSessionStoragePerformanceStatistic.ComponentType.GENERAL);
      sessionStoragePerformanceStatistic.startComponentOperation("c1po", OSessionStoragePerformanceStatistic.ComponentType.GENERAL);
      sessionStoragePerformanceStatistic.incrementPageAccessOnCacheLevel(i % 2 == 0);

      sessionStoragePerformanceStatistic.startComponentOperation("c2po", OSessionStoragePerformanceStatistic.ComponentType.GENERAL);
      sessionStoragePerformanceStatistic.incrementPageAccessOnCacheLevel(true);
      sessionStoragePerformanceStatistic.completeComponentOperation();

      sessionStoragePerformanceStatistic.completeComponentOperation();
      sessionStoragePerformanceStatistic.completeComponentOperation();

    }

    for (int i = 0; i < 50; i++) {
      sessionStoragePerformanceStatistic.incrementPageAccessOnCacheLevel(false);
    }

    Assert.assertEquals(sessionStoragePerformanceStatistic.getCacheHits(), 50);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getCacheHits(null), 50);

    Assert.assertEquals(sessionStoragePerformanceStatistic.getCacheHits("c1po"), 75);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getCacheHits("c2po"), 100);
    Assert.assertEquals(sessionStoragePerformanceStatistic.getCacheHits("c3po"), -1);

    final ODocument doc = sessionStoragePerformanceStatistic.toDocument();

    Assert.assertEquals(doc.field("cacheHits"), Integer.valueOf(50));

    final ODocument docC1PO = doc.<Map<String, ODocument>>field("dataByComponent").get("c1po");
    Assert.assertEquals(docC1PO.field("cacheHits"), Integer.valueOf(75));

    final ODocument docC2PO = doc.<Map<String, ODocument>>field("dataByComponent").get("c2po");
    Assert.assertEquals(docC2PO.field("cacheHits"), Integer.valueOf(100));
  }

  @Test
  public void testPushComponentCounters() {
    final OModifiableInteger counterOne = new OModifiableInteger();

    OSessionStoragePerformanceStatistic sessionStoragePerformanceStatisticOne = new OSessionStoragePerformanceStatistic(100,
        new OSessionStoragePerformanceStatistic.NanoTimer() {

          @Override
          public long getNano() {
            counterOne.increment(100);
            return counterOne.getValue();
          }
        }, -1);

    OSessionStoragePerformanceStatistic.PerformanceCountersHolder performanceCountersHolder = new OSessionStoragePerformanceStatistic.PerformanceCountersHolder();
    final Map<String, OSessionStoragePerformanceStatistic.PerformanceCountersHolder> counters = new HashMap<String, OSessionStoragePerformanceStatistic.PerformanceCountersHolder>();

    sessionStoragePerformanceStatisticOne.pushComponentCounters(counters);
    sessionStoragePerformanceStatisticOne.pushComponentCounters("c3po", performanceCountersHolder);

    sessionStoragePerformanceStatisticOne
        .startComponentOperation("c3po", OSessionStoragePerformanceStatistic.ComponentType.GENERAL);

    sessionStoragePerformanceStatisticOne.incrementPageAccessOnCacheLevel(false);
    sessionStoragePerformanceStatisticOne.incrementPageAccessOnCacheLevel(false);
    sessionStoragePerformanceStatisticOne.incrementPageAccessOnCacheLevel(false);
    sessionStoragePerformanceStatisticOne.incrementPageAccessOnCacheLevel(true);

    sessionStoragePerformanceStatisticOne.startPageReadFromCacheTimer();
    counterOne.increment(50);
    sessionStoragePerformanceStatisticOne.stopPageReadFromCacheTimer();

    sessionStoragePerformanceStatisticOne.startPageReadFromCacheTimer();
    counterOne.increment(50);
    sessionStoragePerformanceStatisticOne.stopPageReadFromCacheTimer();

    sessionStoragePerformanceStatisticOne.startPageReadFromCacheTimer();
    sessionStoragePerformanceStatisticOne.stopPageReadFromCacheTimer();

    sessionStoragePerformanceStatisticOne.startPageReadFromFileTimer();
    sessionStoragePerformanceStatisticOne.stopPageReadFromFileTimer(2);

    sessionStoragePerformanceStatisticOne.startPageWriteInCacheTimer();
    counterOne.increment(200);
    sessionStoragePerformanceStatisticOne.stopPageWriteInCacheTimer();

    sessionStoragePerformanceStatisticOne.completeComponentOperation();

    OSessionStoragePerformanceStatistic sessionStoragePerformanceStatisticTwo = new OSessionStoragePerformanceStatistic(100,
        new OSessionStoragePerformanceStatistic.NanoTimer() {
          private long counter = 0;

          @Override
          public long getNano() {
            return counter += 100;
          }
        }, -1);

    sessionStoragePerformanceStatisticTwo
        .startComponentOperation("c3po", OSessionStoragePerformanceStatistic.ComponentType.GENERAL);
    sessionStoragePerformanceStatisticTwo.incrementPageAccessOnCacheLevel(true);
    sessionStoragePerformanceStatisticTwo.completeComponentOperation();

    sessionStoragePerformanceStatisticTwo
        .startComponentOperation("c1po", OSessionStoragePerformanceStatistic.ComponentType.GENERAL);

    sessionStoragePerformanceStatisticTwo.startPageReadFromCacheTimer();
    sessionStoragePerformanceStatisticTwo.stopPageReadFromCacheTimer();

    sessionStoragePerformanceStatisticTwo.startPageReadFromFileTimer();
    sessionStoragePerformanceStatisticTwo.stopPageReadFromFileTimer(1);

    sessionStoragePerformanceStatisticTwo.startPageWriteInCacheTimer();
    sessionStoragePerformanceStatisticTwo.stopPageWriteInCacheTimer();

    sessionStoragePerformanceStatisticTwo.completeComponentOperation();

    sessionStoragePerformanceStatisticOne.pushComponentCounters("c3po", performanceCountersHolder);

    Assert.assertEquals(performanceCountersHolder.getCacheHits(), sessionStoragePerformanceStatisticOne.getCacheHits());
    Assert.assertEquals(performanceCountersHolder.getAmountOfPagesPerOperation(),
        sessionStoragePerformanceStatisticOne.getAmountOfPagesPerOperation("c3po"));
    Assert.assertEquals(performanceCountersHolder.getAmountOfPagesReadFromCache(),
        sessionStoragePerformanceStatisticOne.getAmountOfPagesReadFromCache("c3po"));
    Assert.assertEquals(performanceCountersHolder.getAmountOfPagesReadFromFile(),
        sessionStoragePerformanceStatisticOne.getAmountOfPagesReadFromFile("c3po"));
    Assert.assertEquals(performanceCountersHolder.getAmountOfPagesWrittenInCache(),
        sessionStoragePerformanceStatisticOne.getAmountOfPagesWrittenInCache("c3po"));
    Assert.assertEquals(performanceCountersHolder.getReadSpeedFromCacheInPages(),
        sessionStoragePerformanceStatisticOne.getReadSpeedFromCacheInPages("c3po"));
    Assert.assertEquals(performanceCountersHolder.getReadSpeedFromFileInPages(),
        sessionStoragePerformanceStatisticOne.getReadSpeedFromFileInPages("c3po"));
    Assert.assertEquals(performanceCountersHolder.getWriteSpeedInCacheInPages(),
        sessionStoragePerformanceStatisticOne.getWriteSpeedInCacheInPages("c3po"));

    performanceCountersHolder = new OSessionStoragePerformanceStatistic.PerformanceCountersHolder();
    sessionStoragePerformanceStatisticTwo.pushComponentCounters("c3po", performanceCountersHolder);

    Assert.assertEquals(performanceCountersHolder.getCacheHits(), sessionStoragePerformanceStatisticTwo.getCacheHits());
    Assert.assertEquals(performanceCountersHolder.getAmountOfPagesPerOperation(),
        sessionStoragePerformanceStatisticTwo.getAmountOfPagesPerOperation("c3po"));
    Assert.assertEquals(performanceCountersHolder.getAmountOfPagesReadFromCache(),
        sessionStoragePerformanceStatisticTwo.getAmountOfPagesReadFromCache("c3po"));
    Assert.assertEquals(performanceCountersHolder.getAmountOfPagesReadFromFile(),
        sessionStoragePerformanceStatisticTwo.getAmountOfPagesReadFromFile("c3po"));
    Assert.assertEquals(performanceCountersHolder.getAmountOfPagesWrittenInCache(),
        sessionStoragePerformanceStatisticTwo.getAmountOfPagesWrittenInCache("c3po"));
    Assert.assertEquals(performanceCountersHolder.getReadSpeedFromCacheInPages(),
        sessionStoragePerformanceStatisticTwo.getReadSpeedFromCacheInPages("c3po"));
    Assert.assertEquals(performanceCountersHolder.getReadSpeedFromFileInPages(),
        sessionStoragePerformanceStatisticTwo.getReadSpeedFromFileInPages("c3po"));
    Assert.assertEquals(performanceCountersHolder.getWriteSpeedInCacheInPages(),
        sessionStoragePerformanceStatisticTwo.getWriteSpeedInCacheInPages("c3po"));

    sessionStoragePerformanceStatisticOne.pushComponentCounters("c3po", performanceCountersHolder);
    Assert.assertEquals(performanceCountersHolder.getCacheHits(), 40);
    Assert.assertEquals(performanceCountersHolder.getAmountOfPagesPerOperation(), 1);
    Assert.assertEquals(performanceCountersHolder.getAmountOfPagesReadFromCache(),
        sessionStoragePerformanceStatisticOne.getAmountOfPagesReadFromCache("c3po"));
    Assert.assertEquals(performanceCountersHolder.getAmountOfPagesReadFromFile(),
        sessionStoragePerformanceStatisticOne.getAmountOfPagesReadFromFile("c3po"));
    Assert.assertEquals(performanceCountersHolder.getAmountOfPagesWrittenInCache(),
        sessionStoragePerformanceStatisticOne.getAmountOfPagesWrittenInCache("c3po"));
    Assert.assertEquals(performanceCountersHolder.getReadSpeedFromCacheInPages(),
        sessionStoragePerformanceStatisticOne.getReadSpeedFromCacheInPages("c3po"));
    Assert.assertEquals(performanceCountersHolder.getReadSpeedFromFileInPages(),
        sessionStoragePerformanceStatisticOne.getReadSpeedFromFileInPages("c3po"));
    Assert.assertEquals(performanceCountersHolder.getWriteSpeedInCacheInPages(),
        sessionStoragePerformanceStatisticOne.getWriteSpeedInCacheInPages("c3po"));

    performanceCountersHolder = new OSessionStoragePerformanceStatistic.PerformanceCountersHolder();
    sessionStoragePerformanceStatisticOne.pushComponentCounters("c3po", performanceCountersHolder);
    sessionStoragePerformanceStatisticTwo.pushComponentCounters("c1po", performanceCountersHolder);

    Assert.assertEquals(performanceCountersHolder.getCacheHits(), sessionStoragePerformanceStatisticOne.getCacheHits());
    Assert.assertEquals(performanceCountersHolder.getAmountOfPagesPerOperation(), 2);
    Assert.assertEquals(performanceCountersHolder.getAmountOfPagesReadFromCache(), 4);
    Assert.assertEquals(performanceCountersHolder.getAmountOfPagesReadFromFile(), 3);
    Assert.assertEquals(performanceCountersHolder.getAmountOfPagesWrittenInCache(), 2);
    Assert.assertEquals(performanceCountersHolder.getReadSpeedFromCacheInPages(), 8000000);
    Assert.assertEquals(performanceCountersHolder.getReadSpeedFromFileInPages(), 15000000);
    Assert.assertEquals(performanceCountersHolder.getWriteSpeedInCacheInPages(), 5000000);

    sessionStoragePerformanceStatisticOne.pushComponentCounters(counters);
    sessionStoragePerformanceStatisticTwo.pushComponentCounters(counters);

    Assert.assertEquals(counters.get("c3po").getCacheHits(), 40);
    Assert.assertEquals(counters.get("c3po").getAmountOfPagesPerOperation(), 1);
    Assert.assertEquals(counters.get("c3po").getAmountOfPagesReadFromCache(), 3);
    Assert.assertEquals(counters.get("c3po").getAmountOfPagesReadFromFile(), 2);
    Assert.assertEquals(counters.get("c3po").getReadSpeedFromCacheInPages(), 7500000);
    Assert.assertEquals(counters.get("c3po").getReadSpeedFromFileInPages(), 20000000);
    Assert.assertEquals(counters.get("c3po").getWriteSpeedInCacheInPages(), 3333333);

    Assert.assertEquals(counters.get("c1po").getCacheHits(), -1);
    Assert.assertEquals(counters.get("c1po").getAmountOfPagesPerOperation(), 1);
    Assert.assertEquals(counters.get("c1po").getAmountOfPagesReadFromCache(), 1);
    Assert.assertEquals(counters.get("c1po").getAmountOfPagesReadFromFile(), 1);
    Assert.assertEquals(counters.get("c1po").getReadSpeedFromCacheInPages(), 10000000);
    Assert.assertEquals(counters.get("c1po").getReadSpeedFromFileInPages(), 10000000);
    Assert.assertEquals(counters.get("c1po").getWriteSpeedInCacheInPages(), 10000000);
  }

  @Test
  public void testSystemCounters() {
    final OModifiableInteger counterOne = new OModifiableInteger();

    OSessionStoragePerformanceStatistic sessionStoragePerformanceStatisticOne = new OSessionStoragePerformanceStatistic(100,
        new OSessionStoragePerformanceStatistic.NanoTimer() {

          @Override
          public long getNano() {
            counterOne.increment(100);
            return counterOne.getValue();
          }
        }, -1);

    final OSessionStoragePerformanceStatistic.PerformanceCountersHolder counters = new OSessionStoragePerformanceStatistic.PerformanceCountersHolder();
    sessionStoragePerformanceStatisticOne.pushSystemCounters(counters);

    sessionStoragePerformanceStatisticOne.incrementPageAccessOnCacheLevel(false);
    sessionStoragePerformanceStatisticOne.incrementPageAccessOnCacheLevel(false);
    sessionStoragePerformanceStatisticOne.incrementPageAccessOnCacheLevel(false);
    sessionStoragePerformanceStatisticOne.incrementPageAccessOnCacheLevel(true);

    sessionStoragePerformanceStatisticOne.startPageReadFromCacheTimer();
    counterOne.increment(50);
    sessionStoragePerformanceStatisticOne.stopPageReadFromCacheTimer();

    sessionStoragePerformanceStatisticOne.startPageReadFromCacheTimer();
    counterOne.increment(50);
    sessionStoragePerformanceStatisticOne.stopPageReadFromCacheTimer();

    sessionStoragePerformanceStatisticOne.startPageReadFromCacheTimer();
    sessionStoragePerformanceStatisticOne.stopPageReadFromCacheTimer();

    sessionStoragePerformanceStatisticOne.startPageReadFromFileTimer();
    sessionStoragePerformanceStatisticOne.stopPageReadFromFileTimer(2);

    sessionStoragePerformanceStatisticOne.startPageWriteInCacheTimer();
    counterOne.increment(200);
    sessionStoragePerformanceStatisticOne.stopPageWriteInCacheTimer();

    sessionStoragePerformanceStatisticOne.startCommitTimer();
    counterOne.increment(100);
    sessionStoragePerformanceStatisticOne.stopCommitTimer();

    OSessionStoragePerformanceStatistic sessionStoragePerformanceStatisticTwo = new OSessionStoragePerformanceStatistic(100,
        new OSessionStoragePerformanceStatistic.NanoTimer() {
          private long counter = 0;

          @Override
          public long getNano() {
            return counter += 100;
          }
        }, -1);

    sessionStoragePerformanceStatisticTwo.incrementPageAccessOnCacheLevel(true);

    sessionStoragePerformanceStatisticTwo.startPageReadFromCacheTimer();
    sessionStoragePerformanceStatisticTwo.stopPageReadFromCacheTimer();

    sessionStoragePerformanceStatisticTwo.startPageReadFromFileTimer();
    sessionStoragePerformanceStatisticTwo.stopPageReadFromFileTimer(1);

    sessionStoragePerformanceStatisticTwo.startPageWriteInCacheTimer();
    sessionStoragePerformanceStatisticTwo.stopPageWriteInCacheTimer();

    sessionStoragePerformanceStatisticTwo.startCommitTimer();
    sessionStoragePerformanceStatisticTwo.stopCommitTimer();

    sessionStoragePerformanceStatisticTwo.completeComponentOperation();

    sessionStoragePerformanceStatisticOne.pushSystemCounters(counters);
    sessionStoragePerformanceStatisticTwo.pushSystemCounters(counters);

    Assert.assertEquals(counters.getCacheHits(), 40);
    Assert.assertEquals(counters.getAmountOfPagesReadFromCache(), 4);
    Assert.assertEquals(counters.getAmountOfPagesReadFromFile(), 3);
    Assert.assertEquals(counters.getAmountOfPagesWrittenInCache(), 2);
    Assert.assertEquals(counters.getReadSpeedFromCacheInPages(), 8000000);
    Assert.assertEquals(counters.getReadSpeedFromFileInPages(), 15000000);
    Assert.assertEquals(counters.getWriteSpeedInCacheInPages(), 5000000);
    Assert.assertEquals(counters.getCommitTime(), 150);
  }

  @Test
  public void testCleanOnSnapshot() {
    final OModifiableInteger counter = new OModifiableInteger(-100);

    OSessionStoragePerformanceStatistic statistic = new OSessionStoragePerformanceStatistic(200,
        new OSessionStoragePerformanceStatistic.NanoTimer() {

          @Override
          public long getNano() {
            counter.increment(100);
            return counter.getValue();
          }
        }, 200);

    OSessionStoragePerformanceStatistic.PerformanceCountersHolder performanceCountersHolder = new OSessionStoragePerformanceStatistic.PerformanceCountersHolder();

    statistic.startComponentOperation("c3po", OSessionStoragePerformanceStatistic.ComponentType.GENERAL);
    statistic.incrementPageAccessOnCacheLevel(false);//100
    counter.setValue(0);
    statistic.completeComponentOperation();//100

    statistic.startComponentOperation("c3po", OSessionStoragePerformanceStatistic.ComponentType.GENERAL);
    statistic.incrementPageAccessOnCacheLevel(true);//200-clear
    counter.setValue(100);
    statistic.completeComponentOperation();//200

    statistic.startComponentOperation("c3po", OSessionStoragePerformanceStatistic.ComponentType.GENERAL);
    statistic.incrementPageAccessOnCacheLevel(true);//300
    counter.setValue(200);
    statistic.completeComponentOperation();//300

    statistic.startComponentOperation("c3po", OSessionStoragePerformanceStatistic.ComponentType.GENERAL);
    statistic.incrementPageAccessOnCacheLevel(true);//400 - clear
    counter.setValue(300);
    statistic.completeComponentOperation();//400

    statistic.pushSystemCounters(performanceCountersHolder);
    Assert.assertEquals(performanceCountersHolder.getCacheHits(), 100);

    performanceCountersHolder = new OSessionStoragePerformanceStatistic.PerformanceCountersHolder();
    statistic.pushComponentCounters("c3po", performanceCountersHolder);
    Assert.assertEquals(performanceCountersHolder.getCacheHits(), 100);

    statistic.startComponentOperation("c3po", OSessionStoragePerformanceStatistic.ComponentType.GENERAL);
    counter.setValue(300);
    statistic.startPageReadFromCacheTimer();//400
    statistic.stopPageReadFromCacheTimer();//500
    counter.setValue(400);
    statistic.startPageReadFromCacheTimer();//500
    statistic.stopPageReadFromCacheTimer();//600 - clear
    counter.setValue(500);
    statistic.completeComponentOperation();//600

    statistic.startComponentOperation("c3po", OSessionStoragePerformanceStatistic.ComponentType.GENERAL);
    counter.setValue(500);
    statistic.startPageReadFromCacheTimer();//600
    counter.increment(100);
    statistic.stopPageReadFromCacheTimer();//800 - clear
    counter.setValue(700);
    statistic.completeComponentOperation();//900

    performanceCountersHolder = new OSessionStoragePerformanceStatistic.PerformanceCountersHolder();
    statistic.pushSystemCounters(performanceCountersHolder);

    Assert.assertEquals(performanceCountersHolder.getAmountOfPagesReadFromCache(), 1);
    Assert.assertEquals(performanceCountersHolder.getReadSpeedFromCacheInPages(), 5000000);

    performanceCountersHolder = new OSessionStoragePerformanceStatistic.PerformanceCountersHolder();
    statistic.pushComponentCounters("c3po", performanceCountersHolder);

    Assert.assertEquals(performanceCountersHolder.getAmountOfPagesReadFromCache(), 1);
    Assert.assertEquals(performanceCountersHolder.getReadSpeedFromCacheInPages(), 5000000);

    statistic.startComponentOperation("c3po", OSessionStoragePerformanceStatistic.ComponentType.GENERAL);
    counter.setValue(700);
    statistic.startPageReadFromFileTimer();//800
    statistic.stopPageReadFromFileTimer(1);//900
    counter.setValue(800);
    statistic.startPageReadFromFileTimer();//900
    statistic.stopPageReadFromFileTimer(1);//1000 - clear
    counter.setValue(900);
    statistic.completeComponentOperation();//1000

    statistic.startComponentOperation("c3po", OSessionStoragePerformanceStatistic.ComponentType.GENERAL);
    counter.setValue(900);
    statistic.startPageReadFromFileTimer();//1000
    statistic.stopPageReadFromFileTimer(2);//1100
    statistic.completeComponentOperation();//1200 - clear

    performanceCountersHolder = new OSessionStoragePerformanceStatistic.PerformanceCountersHolder();
    statistic.pushSystemCounters(performanceCountersHolder);

    Assert.assertEquals(performanceCountersHolder.getAmountOfPagesReadFromFile(), 2);
    Assert.assertEquals(performanceCountersHolder.getReadSpeedFromFileInPages(), 20000000);

    performanceCountersHolder = new OSessionStoragePerformanceStatistic.PerformanceCountersHolder();
    statistic.pushComponentCounters("c3po", performanceCountersHolder);

    Assert.assertEquals(performanceCountersHolder.getAmountOfPagesReadFromFile(), 2);
    Assert.assertEquals(performanceCountersHolder.getReadSpeedFromFileInPages(), 20000000);

    statistic.startComponentOperation("c3po", OSessionStoragePerformanceStatistic.ComponentType.GENERAL);
    counter.setValue(1100);
    statistic.startPageWriteInCacheTimer();//1200
    statistic.stopPageWriteInCacheTimer();//1300
    counter.setValue(1200);
    statistic.startPageWriteInCacheTimer();//1300
    statistic.stopPageWriteInCacheTimer();//1400 - clear
    counter.setValue(1300);
    statistic.completeComponentOperation();//1400

    statistic.startComponentOperation("c3po", OSessionStoragePerformanceStatistic.ComponentType.GENERAL);
    counter.setValue(1300);
    statistic.startPageWriteInCacheTimer();//1400
    counter.increment(100);
    statistic.stopPageWriteInCacheTimer();//1600 - clear
    counter.setValue(1500);
    statistic.completeComponentOperation();//1600

    performanceCountersHolder = new OSessionStoragePerformanceStatistic.PerformanceCountersHolder();
    statistic.pushSystemCounters(performanceCountersHolder);

    Assert.assertEquals(performanceCountersHolder.getAmountOfPagesWrittenInCache(), 1);
    Assert.assertEquals(performanceCountersHolder.getWriteSpeedInCacheInPages(), 5000000);

    performanceCountersHolder = new OSessionStoragePerformanceStatistic.PerformanceCountersHolder();
    statistic.pushComponentCounters("c3po", performanceCountersHolder);

    Assert.assertEquals(performanceCountersHolder.getAmountOfPagesWrittenInCache(), 1);
    Assert.assertEquals(performanceCountersHolder.getWriteSpeedInCacheInPages(), 5000000);

    statistic.startComponentOperation("c3po", OSessionStoragePerformanceStatistic.ComponentType.GENERAL);
    counter.setValue(1500);
    statistic.startCommitTimer();//1600
    statistic.stopCommitTimer();//1700

    counter.setValue(1600);
    statistic.startCommitTimer();//1700
    statistic.stopCommitTimer();//1800 - clear
    counter.setValue(1700);
    statistic.completeComponentOperation();//1800

    statistic.startComponentOperation("c3po", OSessionStoragePerformanceStatistic.ComponentType.GENERAL);
    counter.setValue(1600);
    statistic.startCommitTimer();//1700
    counter.increment(100);
    statistic.stopCommitTimer();//1900
    statistic.completeComponentOperation();//2000 - clear

    performanceCountersHolder = new OSessionStoragePerformanceStatistic.PerformanceCountersHolder();
    statistic.pushSystemCounters(performanceCountersHolder);

    Assert.assertEquals(performanceCountersHolder.getCommitTime(), 200);
  }
}
