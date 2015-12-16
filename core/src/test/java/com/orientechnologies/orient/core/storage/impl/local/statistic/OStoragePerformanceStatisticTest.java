package com.orientechnologies.orient.core.storage.impl.local.statistic;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class OStoragePerformanceStatisticTest {
  public void testReadSpeedFromCache() throws Exception {
    final OStoragePerformanceStatistic storagePerformanceStatistic = new OStoragePerformanceStatistic(1024, "test", 1,
        new OStoragePerformanceStatistic.NanoTimer() {
          private long counter = 0;

          @Override
          public long getNano() {
            return counter += 100;
          }
        });

    Assert.assertFalse(storagePerformanceStatistic.isMeasurementEnabled());
    Assert.assertEquals(storagePerformanceStatistic.getReadSpeedFromCacheInPages(), -1);
    Assert.assertEquals(storagePerformanceStatistic.getReadSpeedFromCacheInMB(), -1);
    Assert.assertEquals(storagePerformanceStatistic.getAmountOfPagesReadFromCache(), 0);

    storagePerformanceStatistic.startMeasurement();

    for (int i = 0; i < 100; i++) {
      storagePerformanceStatistic.startPageReadFromCacheTimer();
      storagePerformanceStatistic.stopPageReadFromCacheTimer();
    }


    Thread.sleep(2000);
    Assert.assertEquals(storagePerformanceStatistic.getReadSpeedFromCacheInPages(), 10000000);
    Assert.assertEquals(storagePerformanceStatistic.getReadSpeedFromCacheInMB(), 10000000 / 1024);
    Assert.assertEquals(storagePerformanceStatistic.getAmountOfPagesReadFromCache(), 100);

    storagePerformanceStatistic.stopMeasurement();

    for (int i = 0; i < 100; i++) {
      storagePerformanceStatistic.startPageReadFromCacheTimer();
      storagePerformanceStatistic.stopPageReadFromCacheTimer();
    }


    Thread.sleep(2000);

    Assert.assertEquals(storagePerformanceStatistic.getReadSpeedFromCacheInPages(), 10000000);
    Assert.assertEquals(storagePerformanceStatistic.getReadSpeedFromCacheInMB(), 10000000 / 1024);
    Assert.assertEquals(storagePerformanceStatistic.getAmountOfPagesReadFromCache(), 100);
    Assert.assertFalse(storagePerformanceStatistic.isMeasurementEnabled());
  }

  public void testReadSpeedFromFile() throws Exception {
    final OStoragePerformanceStatistic storagePerformanceStatistic = new OStoragePerformanceStatistic(1024, "test", 1,
        new OStoragePerformanceStatistic.NanoTimer() {
          private long counter = 0;

          @Override
          public long getNano() {
            return counter += 100;
          }
        });

    Assert.assertFalse(storagePerformanceStatistic.isMeasurementEnabled());
    Assert.assertEquals(storagePerformanceStatistic.getReadSpeedFromFileInPages(), -1);
    Assert.assertEquals(storagePerformanceStatistic.getReadSpeedFromFileInMB(), -1);
    Assert.assertEquals(storagePerformanceStatistic.getAmountOfPagesReadFromFile(), 0);

    storagePerformanceStatistic.startMeasurement();

    for (int i = 0; i < 100; i++) {
      storagePerformanceStatistic.startPageReadFromFileTimer();
      storagePerformanceStatistic.stopPageReadFromFileTimer(10);
    }


    Thread.sleep(2000);
    Assert.assertEquals(storagePerformanceStatistic.getReadSpeedFromFileInPages(), 100000000);
    Assert.assertEquals(storagePerformanceStatistic.getReadSpeedFromFileInMB(), 100000000 / 1024);
    Assert.assertEquals(storagePerformanceStatistic.getAmountOfPagesReadFromFile(), 1000);

    storagePerformanceStatistic.stopMeasurement();

    for (int i = 0; i < 100; i++) {
      storagePerformanceStatistic.startPageReadFromFileTimer();
      storagePerformanceStatistic.stopPageReadFromFileTimer(10);
    }

    Thread.sleep(2000);

    Assert.assertEquals(storagePerformanceStatistic.getReadSpeedFromFileInPages(), 100000000);
    Assert.assertEquals(storagePerformanceStatistic.getReadSpeedFromFileInMB(), 100000000 / 1024);
    Assert.assertEquals(storagePerformanceStatistic.getAmountOfPagesReadFromFile(), 1000);
    Assert.assertFalse(storagePerformanceStatistic.isMeasurementEnabled());

  }

  public void testWriteSpeedToCache() throws Exception {
    final OStoragePerformanceStatistic storagePerformanceStatistic = new OStoragePerformanceStatistic(1024, "test", 1,
        new OStoragePerformanceStatistic.NanoTimer() {
          private long counter = 0;

          @Override
          public long getNano() {
            return counter += 100;
          }
        });

    Assert.assertFalse(storagePerformanceStatistic.isMeasurementEnabled());
    Assert.assertEquals(storagePerformanceStatistic.getWriteSpeedInCacheInPages(), -1);
    Assert.assertEquals(storagePerformanceStatistic.getWriteSpeedInCacheInMB(), -1);
    Assert.assertEquals(storagePerformanceStatistic.getAmountOfPagesWrittenToCache(), 0);


    storagePerformanceStatistic.startMeasurement();

    for (int i = 0; i < 100; i++) {
      storagePerformanceStatistic.startPageWriteToCacheTimer();
      ;
      storagePerformanceStatistic.stopPageWriteToCacheTimer();
    }


    Thread.sleep(2000);
    Assert.assertEquals(storagePerformanceStatistic.getWriteSpeedInCacheInPages(), 10000000);
    Assert.assertEquals(storagePerformanceStatistic.getWriteSpeedInCacheInMB(), 10000000 / 1024);
    Assert.assertEquals(storagePerformanceStatistic.getAmountOfPagesWrittenToCache(), 100);

    storagePerformanceStatistic.stopMeasurement();

    for (int i = 0; i < 100; i++) {
      storagePerformanceStatistic.startPageWriteToCacheTimer();
      ;
      storagePerformanceStatistic.stopPageWriteToCacheTimer();
    }


    Thread.sleep(2000);

    Assert.assertEquals(storagePerformanceStatistic.getWriteSpeedInCacheInPages(), 10000000);
    Assert.assertEquals(storagePerformanceStatistic.getWriteSpeedInCacheInMB(), 10000000 / 1024);
    Assert.assertEquals(storagePerformanceStatistic.getAmountOfPagesWrittenToCache(), 100);
    Assert.assertFalse(storagePerformanceStatistic.isMeasurementEnabled());
  }

  public void testWriteSpeedToFile() throws Exception {
    final OStoragePerformanceStatistic storagePerformanceStatistic = new OStoragePerformanceStatistic(1024, "test", 1,
        new OStoragePerformanceStatistic.NanoTimer() {
          private long counter = 0;

          @Override
          public long getNano() {
            return counter += 100;
          }
        });

    Assert.assertFalse(storagePerformanceStatistic.isMeasurementEnabled());
    Assert.assertEquals(storagePerformanceStatistic.getWriteSpeedInFileInPages(), -1);
    Assert.assertEquals(storagePerformanceStatistic.getWriteSpeedInFileInMB(), -1);
    Assert.assertEquals(storagePerformanceStatistic.getAmountOfPagesWrittenToFile(), 0);

    storagePerformanceStatistic.startMeasurement();

    for (int i = 0; i < 100; i++) {
      storagePerformanceStatistic.startPageWriteToFileTimer();
      ;
      storagePerformanceStatistic.stopPageWriteToFileTimer();
    }


    Thread.sleep(2000);
    Assert.assertEquals(storagePerformanceStatistic.getWriteSpeedInFileInPages(), 10000000);
    Assert.assertEquals(storagePerformanceStatistic.getWriteSpeedInFileInMB(), 10000000 / 1024);
    Assert.assertEquals(storagePerformanceStatistic.getAmountOfPagesWrittenToFile(), 100);

    storagePerformanceStatistic.stopMeasurement();

    for (int i = 0; i < 100; i++) {
      storagePerformanceStatistic.startPageWriteToFileTimer();
      ;
      storagePerformanceStatistic.stopPageWriteToFileTimer();
    }

    Thread.sleep(2000);

    Assert.assertEquals(storagePerformanceStatistic.getWriteSpeedInFileInPages(), 10000000);
    Assert.assertEquals(storagePerformanceStatistic.getWriteSpeedInFileInMB(), 10000000 / 1024);
    Assert.assertEquals(storagePerformanceStatistic.getAmountOfPagesWrittenToFile(), 100);

    Assert.assertFalse(storagePerformanceStatistic.isMeasurementEnabled());
  }

  public void testCommitTime() throws Exception {
    final OStoragePerformanceStatistic storagePerformanceStatistic = new OStoragePerformanceStatistic(1024, "test", 1,
        new OStoragePerformanceStatistic.NanoTimer() {
          private long counter = 0;

          @Override
          public long getNano() {
            return counter += 100;
          }
        });

    Assert.assertFalse(storagePerformanceStatistic.isMeasurementEnabled());
    Assert.assertEquals(storagePerformanceStatistic.getCommitTimeAvg(), -1);

    storagePerformanceStatistic.startMeasurement();

    for (int i = 0; i < 100; i++) {
      storagePerformanceStatistic.startCommitTimer();
      storagePerformanceStatistic.stopCommitTimer();
    }


    Thread.sleep(2000);
    Assert.assertEquals(storagePerformanceStatistic.getCommitTimeAvg(), 100);

    storagePerformanceStatistic.stopMeasurement();

    Thread.sleep(2000);

    Assert.assertEquals(storagePerformanceStatistic.getCommitTimeAvg(), 100);
    Assert.assertFalse(storagePerformanceStatistic.isMeasurementEnabled());
  }
}
