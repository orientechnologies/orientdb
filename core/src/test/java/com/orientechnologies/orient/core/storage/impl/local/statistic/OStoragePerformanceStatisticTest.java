package com.orientechnologies.orient.core.storage.impl.local.statistic;

import org.testng.Assert;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

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

    storagePerformanceStatistic.startCommitTimer();
    for (int i = 0; i < 100; i++) {
      storagePerformanceStatistic.startPageReadFromCacheTimer();
      storagePerformanceStatistic.stopPageReadFromCacheTimer();
    }
    storagePerformanceStatistic.stopCommitTimer();


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

  public void testReadSpeedFromCacheMBean() throws Exception {
    final OStoragePerformanceStatistic storagePerformanceStatistic = new OStoragePerformanceStatistic(1024, "test", 1,
        new OStoragePerformanceStatistic.NanoTimer() {
          private long counter = 0;

          @Override
          public long getNano() {
            return counter += 100;
          }
        });

    storagePerformanceStatistic.registerMBean();

    final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    final ObjectName objectName = new ObjectName("com.orientechnologies.orient.core.storage.impl.local." +
        "statistic:type=OStoragePerformanceStatisticMXBean,name=test,id=1");

    Assert.assertTrue(mbs.isRegistered(objectName));

    storagePerformanceStatistic.registerMBean();

    final OStoragePerformanceStatisticMXBean mxBean =
        MBeanServerInvocationHandler.newProxyInstance(mbs, objectName, OStoragePerformanceStatisticMXBean.class, false);

    Assert.assertFalse(mxBean.isMeasurementEnabled());
    Assert.assertEquals(mxBean.getReadSpeedFromCacheInPages(), -1);
    Assert.assertEquals(mxBean.getReadSpeedFromCacheInMB(), -1);
    Assert.assertEquals(mxBean.getAmountOfPagesReadFromCache(), 0);

    mxBean.startMeasurement();

    storagePerformanceStatistic.startCommitTimer();
    for (int i = 0; i < 100; i++) {
      storagePerformanceStatistic.startPageReadFromCacheTimer();
      storagePerformanceStatistic.stopPageReadFromCacheTimer();
    }
    storagePerformanceStatistic.stopCommitTimer();


    Thread.sleep(2000);
    Assert.assertEquals(mxBean.getReadSpeedFromCacheInPages(), 10000000);
    Assert.assertEquals(mxBean.getReadSpeedFromCacheInMB(), 10000000 / 1024);
    Assert.assertEquals(mxBean.getAmountOfPagesReadFromCache(), 100);

    mxBean.stopMeasurement();

    for (int i = 0; i < 100; i++) {
      storagePerformanceStatistic.startPageReadFromCacheTimer();
      storagePerformanceStatistic.stopPageReadFromCacheTimer();
    }


    Thread.sleep(2000);

    Assert.assertEquals(mxBean.getReadSpeedFromCacheInPages(), 10000000);
    Assert.assertEquals(mxBean.getReadSpeedFromCacheInMB(), 10000000 / 1024);
    Assert.assertEquals(mxBean.getAmountOfPagesReadFromCache(), 100);
    Assert.assertFalse(mxBean.isMeasurementEnabled());

    storagePerformanceStatistic.unregisterMBean();
    Assert.assertFalse(mbs.isRegistered(objectName));

    storagePerformanceStatistic.unregisterMBean();
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

    storagePerformanceStatistic.startCommitTimer();
    for (int i = 0; i < 100; i++) {
      storagePerformanceStatistic.startPageReadFromFileTimer();
      storagePerformanceStatistic.stopPageReadFromFileTimer(10);
    }
    storagePerformanceStatistic.stopCommitTimer();


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

  public void testReadSpeedFromFileMBean() throws Exception {
    final OStoragePerformanceStatistic storagePerformanceStatistic = new OStoragePerformanceStatistic(1024, "test", 1,
        new OStoragePerformanceStatistic.NanoTimer() {
          private long counter = 0;

          @Override
          public long getNano() {
            return counter += 100;
          }
        });

    storagePerformanceStatistic.registerMBean();

    final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    final ObjectName objectName = new ObjectName("com.orientechnologies.orient.core.storage.impl.local." +
        "statistic:type=OStoragePerformanceStatisticMXBean,name=test,id=1");

    Assert.assertTrue(mbs.isRegistered(objectName));

    storagePerformanceStatistic.registerMBean();

    final OStoragePerformanceStatisticMXBean mxBean =
        MBeanServerInvocationHandler.newProxyInstance(mbs, objectName, OStoragePerformanceStatisticMXBean.class, false);


    Assert.assertFalse(mxBean.isMeasurementEnabled());
    Assert.assertEquals(mxBean.getReadSpeedFromFileInPages(), -1);
    Assert.assertEquals(mxBean.getReadSpeedFromFileInMB(), -1);
    Assert.assertEquals(mxBean.getAmountOfPagesReadFromFile(), 0);

    mxBean.startMeasurement();

    storagePerformanceStatistic.startCommitTimer();
    for (int i = 0; i < 100; i++) {
      storagePerformanceStatistic.startPageReadFromFileTimer();
      storagePerformanceStatistic.stopPageReadFromFileTimer(10);
    }
    storagePerformanceStatistic.stopCommitTimer();


    Thread.sleep(2000);
    Assert.assertEquals(mxBean.getReadSpeedFromFileInPages(), 100000000);
    Assert.assertEquals(mxBean.getReadSpeedFromFileInMB(), 100000000 / 1024);
    Assert.assertEquals(mxBean.getAmountOfPagesReadFromFile(), 1000);

    mxBean.stopMeasurement();

    for (int i = 0; i < 100; i++) {
      storagePerformanceStatistic.startPageReadFromFileTimer();
      storagePerformanceStatistic.stopPageReadFromFileTimer(10);
    }

    Thread.sleep(2000);

    Assert.assertEquals(mxBean.getReadSpeedFromFileInPages(), 100000000);
    Assert.assertEquals(mxBean.getReadSpeedFromFileInMB(), 100000000 / 1024);
    Assert.assertEquals(mxBean.getAmountOfPagesReadFromFile(), 1000);
    Assert.assertFalse(mxBean.isMeasurementEnabled());

    storagePerformanceStatistic.unregisterMBean();
    Assert.assertFalse(mbs.isRegistered(objectName));

    storagePerformanceStatistic.unregisterMBean();
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

    storagePerformanceStatistic.startCommitTimer();
    for (int i = 0; i < 100; i++) {
      storagePerformanceStatistic.startPageWriteToCacheTimer();
      storagePerformanceStatistic.stopPageWriteToCacheTimer();
    }
    storagePerformanceStatistic.stopCommitTimer();


    Thread.sleep(2000);
    Assert.assertEquals(storagePerformanceStatistic.getWriteSpeedInCacheInPages(), 10000000);
    Assert.assertEquals(storagePerformanceStatistic.getWriteSpeedInCacheInMB(), 10000000 / 1024);
    Assert.assertEquals(storagePerformanceStatistic.getAmountOfPagesWrittenToCache(), 100);

    storagePerformanceStatistic.stopMeasurement();

    for (int i = 0; i < 100; i++) {
      storagePerformanceStatistic.startPageWriteToCacheTimer();
      storagePerformanceStatistic.stopPageWriteToCacheTimer();
    }


    Thread.sleep(2000);

    Assert.assertEquals(storagePerformanceStatistic.getWriteSpeedInCacheInPages(), 10000000);
    Assert.assertEquals(storagePerformanceStatistic.getWriteSpeedInCacheInMB(), 10000000 / 1024);
    Assert.assertEquals(storagePerformanceStatistic.getAmountOfPagesWrittenToCache(), 100);
    Assert.assertFalse(storagePerformanceStatistic.isMeasurementEnabled());
  }

  public void testWriteSpeedToCacheMBean() throws Exception {
    final OStoragePerformanceStatistic storagePerformanceStatistic = new OStoragePerformanceStatistic(1024, "test", 1,
        new OStoragePerformanceStatistic.NanoTimer() {
          private long counter = 0;

          @Override
          public long getNano() {
            return counter += 100;
          }
        });

    storagePerformanceStatistic.registerMBean();

    final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    final ObjectName objectName = new ObjectName("com.orientechnologies.orient.core.storage.impl.local." +
        "statistic:type=OStoragePerformanceStatisticMXBean,name=test,id=1");

    Assert.assertTrue(mbs.isRegistered(objectName));

    storagePerformanceStatistic.registerMBean();

    final OStoragePerformanceStatisticMXBean mxBean =
        MBeanServerInvocationHandler.newProxyInstance(mbs, objectName, OStoragePerformanceStatisticMXBean.class, false);

    Assert.assertFalse(mxBean.isMeasurementEnabled());
    Assert.assertEquals(mxBean.getWriteSpeedInCacheInPages(), -1);
    Assert.assertEquals(mxBean.getWriteSpeedInCacheInMB(), -1);
    Assert.assertEquals(mxBean.getAmountOfPagesWrittenToCache(), 0);

    mxBean.startMeasurement();

    storagePerformanceStatistic.startCommitTimer();
    for (int i = 0; i < 100; i++) {
      storagePerformanceStatistic.startPageWriteToCacheTimer();
      storagePerformanceStatistic.stopPageWriteToCacheTimer();
    }
    storagePerformanceStatistic.stopCommitTimer();


    Thread.sleep(2000);
    Assert.assertEquals(mxBean.getWriteSpeedInCacheInPages(), 10000000);
    Assert.assertEquals(mxBean.getWriteSpeedInCacheInMB(), 10000000 / 1024);
    Assert.assertEquals(mxBean.getAmountOfPagesWrittenToCache(), 100);

    mxBean.stopMeasurement();

    for (int i = 0; i < 100; i++) {
      storagePerformanceStatistic.startPageWriteToCacheTimer();
      storagePerformanceStatistic.stopPageWriteToCacheTimer();
    }

    Thread.sleep(2000);

    Assert.assertEquals(mxBean.getWriteSpeedInCacheInPages(), 10000000);
    Assert.assertEquals(mxBean.getWriteSpeedInCacheInMB(), 10000000 / 1024);
    Assert.assertEquals(mxBean.getAmountOfPagesWrittenToCache(), 100);
    Assert.assertFalse(mxBean.isMeasurementEnabled());

    storagePerformanceStatistic.unregisterMBean();
    Assert.assertFalse(mbs.isRegistered(objectName));

    storagePerformanceStatistic.unregisterMBean();
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

    storagePerformanceStatistic.startCommitTimer();
    for (int i = 0; i < 100; i++) {
      storagePerformanceStatistic.startPageWriteToFileTimer();
      storagePerformanceStatistic.stopPageWriteToFileTimer();
    }
    storagePerformanceStatistic.stopCommitTimer();


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

  public void testWriteSpeedToFileMBean() throws Exception {
    final OStoragePerformanceStatistic storagePerformanceStatistic = new OStoragePerformanceStatistic(1024, "test", 1,
        new OStoragePerformanceStatistic.NanoTimer() {
          private long counter = 0;

          @Override
          public long getNano() {
            return counter += 100;
          }
        });

    storagePerformanceStatistic.registerMBean();

    final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    final ObjectName objectName = new ObjectName("com.orientechnologies.orient.core.storage.impl.local." +
        "statistic:type=OStoragePerformanceStatisticMXBean,name=test,id=1");

    Assert.assertTrue(mbs.isRegistered(objectName));

    storagePerformanceStatistic.registerMBean();

    final OStoragePerformanceStatisticMXBean mxBean =
        MBeanServerInvocationHandler.newProxyInstance(mbs, objectName, OStoragePerformanceStatisticMXBean.class, false);

    Assert.assertFalse(mxBean.isMeasurementEnabled());
    Assert.assertEquals(mxBean.getWriteSpeedInFileInPages(), -1);
    Assert.assertEquals(mxBean.getWriteSpeedInFileInMB(), -1);
    Assert.assertEquals(mxBean.getAmountOfPagesWrittenToFile(), 0);

    mxBean.startMeasurement();

    storagePerformanceStatistic.startCommitTimer();
    for (int i = 0; i < 100; i++) {
      storagePerformanceStatistic.startPageWriteToFileTimer();
      storagePerformanceStatistic.stopPageWriteToFileTimer();
    }
    storagePerformanceStatistic.stopCommitTimer();


    Thread.sleep(2000);
    Assert.assertEquals(mxBean.getWriteSpeedInFileInPages(), 10000000);
    Assert.assertEquals(mxBean.getWriteSpeedInFileInMB(), 10000000 / 1024);
    Assert.assertEquals(mxBean.getAmountOfPagesWrittenToFile(), 100);

    mxBean.stopMeasurement();

    for (int i = 0; i < 100; i++) {
      storagePerformanceStatistic.startPageWriteToFileTimer();
      ;
      storagePerformanceStatistic.stopPageWriteToFileTimer();
    }

    Thread.sleep(2000);

    Assert.assertEquals(mxBean.getWriteSpeedInFileInPages(), 10000000);
    Assert.assertEquals(mxBean.getWriteSpeedInFileInMB(), 10000000 / 1024);
    Assert.assertEquals(mxBean.getAmountOfPagesWrittenToFile(), 100);

    Assert.assertFalse(mxBean.isMeasurementEnabled());

    storagePerformanceStatistic.unregisterMBean();
    Assert.assertFalse(mbs.isRegistered(objectName));

    storagePerformanceStatistic.unregisterMBean();
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

    storagePerformanceStatistic.startPageReadFromCacheTimer();
    for (int i = 0; i < 100; i++) {
      storagePerformanceStatistic.startCommitTimer();
      storagePerformanceStatistic.stopCommitTimer();
    }
    storagePerformanceStatistic.stopPageReadFromCacheTimer();


    Thread.sleep(2000);
    Assert.assertEquals(storagePerformanceStatistic.getCommitTimeAvg(), 100);

    storagePerformanceStatistic.stopMeasurement();

    Thread.sleep(2000);

    Assert.assertEquals(storagePerformanceStatistic.getCommitTimeAvg(), 100);
    Assert.assertFalse(storagePerformanceStatistic.isMeasurementEnabled());
  }

  public void testCommitTimeMBean() throws Exception {
    final OStoragePerformanceStatistic storagePerformanceStatistic = new OStoragePerformanceStatistic(1024, "test", 1,
        new OStoragePerformanceStatistic.NanoTimer() {
          private long counter = 0;

          @Override
          public long getNano() {
            return counter += 100;
          }
        });

    storagePerformanceStatistic.registerMBean();

    final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    final ObjectName objectName = new ObjectName("com.orientechnologies.orient.core.storage.impl.local." +
        "statistic:type=OStoragePerformanceStatisticMXBean,name=test,id=1");

    Assert.assertTrue(mbs.isRegistered(objectName));

    storagePerformanceStatistic.registerMBean();

    final OStoragePerformanceStatisticMXBean mxBean =
        MBeanServerInvocationHandler.newProxyInstance(mbs, objectName, OStoragePerformanceStatisticMXBean.class, false);

    Assert.assertFalse(mxBean.isMeasurementEnabled());
    Assert.assertEquals(mxBean.getCommitTimeAvg(), -1);

    mxBean.startMeasurement();

    storagePerformanceStatistic.startPageReadFromCacheTimer();
    for (int i = 0; i < 100; i++) {
      storagePerformanceStatistic.startCommitTimer();
      storagePerformanceStatistic.stopCommitTimer();
    }
    storagePerformanceStatistic.stopPageReadFromCacheTimer();


    Thread.sleep(2000);
    Assert.assertEquals(mxBean.getCommitTimeAvg(), 100);

    mxBean.stopMeasurement();

    Thread.sleep(2000);

    Assert.assertEquals(mxBean.getCommitTimeAvg(), 100);
    Assert.assertFalse(mxBean.isMeasurementEnabled());

    storagePerformanceStatistic.unregisterMBean();
    Assert.assertFalse(mbs.isRegistered(objectName));

    storagePerformanceStatistic.unregisterMBean();
  }


  public void testCacheHitCount() throws Exception {
    final OStoragePerformanceStatistic storagePerformanceStatistic = new OStoragePerformanceStatistic(1024, "test", 1,
        new OStoragePerformanceStatistic.NanoTimer() {
          private long counter = 0;

          @Override
          public long getNano() {
            return counter += 100;
          }
        });

    Assert.assertFalse(storagePerformanceStatistic.isMeasurementEnabled());
    Assert.assertEquals(storagePerformanceStatistic.getCacheHits(), -1);

    storagePerformanceStatistic.startMeasurement();


    for (int i = 0; i < 100; i++) {
      storagePerformanceStatistic.incrementPageAccessOnCacheLevel();
      if (i % 2 == 0)
        storagePerformanceStatistic.incrementCacheHit();
    }

    Thread.sleep(2000);
    Assert.assertEquals(storagePerformanceStatistic.getCacheHits(), 50);

    storagePerformanceStatistic.stopMeasurement();

    for (int i = 0; i < 100; i++) {
      storagePerformanceStatistic.incrementPageAccessOnCacheLevel();
      storagePerformanceStatistic.incrementCacheHit();
    }


    Thread.sleep(2000);

    Assert.assertEquals(storagePerformanceStatistic.getCacheHits(), 50);
    Assert.assertFalse(storagePerformanceStatistic.isMeasurementEnabled());
  }

  public void testCacheHitCountMBean() throws Exception {
    final OStoragePerformanceStatistic storagePerformanceStatistic = new OStoragePerformanceStatistic(1024, "test", 1,
        new OStoragePerformanceStatistic.NanoTimer() {
          private long counter = 0;

          @Override
          public long getNano() {
            return counter += 100;
          }
        });

    storagePerformanceStatistic.registerMBean();

    final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    final ObjectName objectName = new ObjectName("com.orientechnologies.orient.core.storage.impl.local." +
        "statistic:type=OStoragePerformanceStatisticMXBean,name=test,id=1");

    Assert.assertTrue(mbs.isRegistered(objectName));

    storagePerformanceStatistic.registerMBean();

    final OStoragePerformanceStatisticMXBean mxBean =
        MBeanServerInvocationHandler.newProxyInstance(mbs, objectName, OStoragePerformanceStatisticMXBean.class, false);

    Assert.assertFalse(mxBean.isMeasurementEnabled());
    Assert.assertEquals(mxBean.getCacheHits(), -1);

    mxBean.startMeasurement();

    for (int i = 0; i < 100; i++) {
      storagePerformanceStatistic.incrementPageAccessOnCacheLevel();
      if (i % 2 == 0)
        storagePerformanceStatistic.incrementCacheHit();
    }

    Thread.sleep(2000);
    Assert.assertEquals(mxBean.getCacheHits(), 50);

    mxBean.stopMeasurement();

    for (int i = 0; i < 100; i++) {
      storagePerformanceStatistic.incrementPageAccessOnCacheLevel();
      storagePerformanceStatistic.incrementCacheHit();
    }


    Thread.sleep(2000);

    Assert.assertEquals(mxBean.getCacheHits(), 50);
    Assert.assertFalse(mxBean.isMeasurementEnabled());

    storagePerformanceStatistic.unregisterMBean();
    Assert.assertFalse(mbs.isRegistered(objectName));

    storagePerformanceStatistic.unregisterMBean();
  }

}
