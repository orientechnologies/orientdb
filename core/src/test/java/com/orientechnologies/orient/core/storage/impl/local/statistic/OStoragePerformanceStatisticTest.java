package com.orientechnologies.orient.core.storage.impl.local.statistic;

import org.testng.Assert;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Test
public class OStoragePerformanceStatisticTest {
  public void testReadSpeedFromCache() throws Exception {
    final AssertDataFunction assertDataFunction = new AssertDataFunction() {
      @Override
      public void assertData(long[] data) throws AssertionError {
        Assert.assertEquals(data[0], 10000000);
        Assert.assertEquals(data[1], 10000000 / 1024);
      }
    };

    final GetDataFunction getDataFunction = new GetDataFunction() {
      @Override
      public long[] getData(OStoragePerformanceStatisticMXBean storagePerformanceStatistic) {
        final long[] result = new long[2];
        result[0] = storagePerformanceStatistic.getReadSpeedFromCacheInPages();
        result[1] = storagePerformanceStatistic.getReadSpeedFromCacheInMB();
        return result;
      }
    };

    final LoadLoop loadLoop = new LoadLoop() {
      @Override
      public void execute(OStoragePerformanceStatistic storagePerformanceStatistic, boolean measurementStopped) {
        storagePerformanceStatistic.startCommitTimer();
        storagePerformanceStatistic.startPageReadFromCacheTimer();
        storagePerformanceStatistic.stopPageReadFromCacheTimer();
        storagePerformanceStatistic.stopCommitTimer();
      }
    };

    testNoMBeanCase("testReadSpeedFromCache", loadLoop, getDataFunction, assertDataFunction);
    testMBeanCase("testReadSpeedFromCache", loadLoop, getDataFunction, assertDataFunction);
  }

  public void testReadSpeedFromFile() throws Exception {
    final AssertDataFunction assertDataFunction = new AssertDataFunction() {
      @Override
      public void assertData(long[] data) throws AssertionError {
        Assert.assertEquals(data[0], 100000000);
        Assert.assertEquals(data[1], 100000000 / 1024);
      }
    };

    final GetDataFunction getDataFunction = new GetDataFunction() {
      @Override
      public long[] getData(OStoragePerformanceStatisticMXBean storagePerformanceStatistic) {
        final long[] result = new long[2];
        result[0] = storagePerformanceStatistic.getReadSpeedFromFileInPages();
        result[1] = storagePerformanceStatistic.getReadSpeedFromFileInMB();
        return result;
      }
    };

    final LoadLoop loadLoop = new LoadLoop() {
      @Override
      public void execute(OStoragePerformanceStatistic storagePerformanceStatistic, boolean measurementStopped) {
        storagePerformanceStatistic.startCommitTimer();
        storagePerformanceStatistic.startPageReadFromFileTimer();
        storagePerformanceStatistic.stopPageReadFromFileTimer(10);
        storagePerformanceStatistic.stopCommitTimer();
      }
    };

    testNoMBeanCase("testReadSpeedFromFile", loadLoop, getDataFunction, assertDataFunction);
    testMBeanCase("testReadSpeedFromFile", loadLoop, getDataFunction, assertDataFunction);
  }

  public void testWriteSpeedToCache() throws Exception {
    final AssertDataFunction assertDataFunction = new AssertDataFunction() {
      @Override
      public void assertData(long[] data) throws AssertionError {
        Assert.assertEquals(data[0], 10000000);
        Assert.assertEquals(data[1], 10000000 / 1024);
      }
    };

    final GetDataFunction getDataFunction = new GetDataFunction() {
      @Override
      public long[] getData(OStoragePerformanceStatisticMXBean storagePerformanceStatistic) {
        final long[] result = new long[2];
        result[0] = storagePerformanceStatistic.getWriteSpeedInCacheInPages();
        result[1] = storagePerformanceStatistic.getWriteSpeedInCacheInMB();
        return result;
      }
    };

    final LoadLoop loadLoop = new LoadLoop() {
      @Override
      public void execute(OStoragePerformanceStatistic storagePerformanceStatistic, boolean measurementStopped) {
        storagePerformanceStatistic.startCommitTimer();
        storagePerformanceStatistic.startPageWriteToCacheTimer();
        storagePerformanceStatistic.stopPageWriteToCacheTimer();
        storagePerformanceStatistic.stopCommitTimer();
      }
    };

    testNoMBeanCase("testWriteSpeedToCache", loadLoop, getDataFunction, assertDataFunction);
    testMBeanCase("testWriteSpeedToCache", loadLoop, getDataFunction, assertDataFunction);
  }

  public void testWriteSpeedToFile() throws Exception {
    final AssertDataFunction assertDataFunction = new AssertDataFunction() {
      @Override
      public void assertData(long[] data) throws AssertionError {
        Assert.assertEquals(data[0], 10000000);
        Assert.assertEquals(data[1], 10000000 / 1024);
      }
    };

    final GetDataFunction getDataFunction = new GetDataFunction() {
      @Override
      public long[] getData(OStoragePerformanceStatisticMXBean storagePerformanceStatistic) {
        final long[] result = new long[2];
        result[0] = storagePerformanceStatistic.getWriteSpeedInFileInPages();
        result[1] = storagePerformanceStatistic.getWriteSpeedInFileInMB();
        return result;
      }
    };

    final LoadLoop loadLoop = new LoadLoop() {
      @Override
      public void execute(OStoragePerformanceStatistic storagePerformanceStatistic, boolean measurementStopped) {
        storagePerformanceStatistic.startCommitTimer();
        storagePerformanceStatistic.startPageWriteToFileTimer();
        storagePerformanceStatistic.stopPageWriteToFileTimer();
        storagePerformanceStatistic.stopCommitTimer();
      }
    };

    testNoMBeanCase("testWriteSpeedToCache", loadLoop, getDataFunction, assertDataFunction);
    testMBeanCase("testWriteSpeedToCache", loadLoop, getDataFunction, assertDataFunction);
  }

  public void testCommitTime() throws Exception {

    final AssertDataFunction assertDataFunction = new AssertDataFunction() {
      @Override
      public void assertData(long[] data) throws AssertionError {
        Assert.assertEquals(data[0], 100);
      }
    };

    final GetDataFunction getDataFunction = new GetDataFunction() {
      @Override
      public long[] getData(OStoragePerformanceStatisticMXBean storagePerformanceStatistic) {
        final long[] result = new long[1];
        result[0] = storagePerformanceStatistic.getCommitTimeAvg();
        return result;
      }
    };

    final LoadLoop loadLoop = new LoadLoop() {
      @Override
      public void execute(OStoragePerformanceStatistic storagePerformanceStatistic, boolean measurementStopped) {
        storagePerformanceStatistic.startPageReadFromCacheTimer();
        storagePerformanceStatistic.startCommitTimer();
        storagePerformanceStatistic.stopCommitTimer();
        storagePerformanceStatistic.stopPageReadFromCacheTimer();
      }
    };

    testNoMBeanCase("testCommitTime", loadLoop, getDataFunction, assertDataFunction);
    testMBeanCase("testCommitTime", loadLoop, getDataFunction, assertDataFunction);
  }

  public void testCacheHitCount() throws Exception {

    final AssertDataFunction assertDataFunction = new AssertDataFunction() {
      @Override
      public void assertData(long[] data) throws AssertionError {
        Assert.assertTrue(data[0] < 60 && data[0] > 40);
      }
    };

    final GetDataFunction getDataFunction = new GetDataFunction() {
      @Override
      public long[] getData(OStoragePerformanceStatisticMXBean storagePerformanceStatistic) {
        final long[] result = new long[1];
        result[0] = storagePerformanceStatistic.getCacheHits();
        return result;
      }
    };

    final LoadLoop loadLoop = new LoadLoop() {
      long counter;

      @Override
      public void execute(OStoragePerformanceStatistic storagePerformanceStatistic, boolean measurementStopped) {
        if (!measurementStopped)
          storagePerformanceStatistic.incrementPageAccessOnCacheLevel(counter % 2 == 0);
        else
          storagePerformanceStatistic.incrementPageAccessOnCacheLevel(true);

        counter++;
      }
    };

    testNoMBeanCase("testCacheHitCount", loadLoop, getDataFunction, assertDataFunction);
    testMBeanCase("testCacheHitCount", loadLoop, getDataFunction, assertDataFunction);
  }

  private void testNoMBeanCase(String testName, final LoadLoop loadLoop, final GetDataFunction getDataFunction,
      final AssertDataFunction assertDataFunction) throws Exception {
    final AtomicLong nanoIncrement = new AtomicLong();
    nanoIncrement.set(100);

    final OStoragePerformanceStatistic storagePerformanceStatistic = new OStoragePerformanceStatistic(1024, "test", 1,
        new OStoragePerformanceStatistic.NanoTimer() {
          private long counter = 0;

          @Override
          public long getNano() {
            return counter += nanoIncrement.get();
          }
        }, 100000); // take measurements during 100 ms

    Assert.assertFalse(storagePerformanceStatistic.isMeasurementEnabled());

    long[] speedData = getDataFunction.getData(storagePerformanceStatistic);

    for (long datum : speedData) {
      Assert.assertEquals(datum, -1);
    }

    storagePerformanceStatistic.startMeasurement();

    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicBoolean measurementStopped = new AtomicBoolean();

    final ExecutorService executor = Executors.newSingleThreadExecutor();

    final Future<Void> future = executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        while (!stop.get()) {
          loadLoop.execute(storagePerformanceStatistic, measurementStopped.get());
        }
        return null;
      }
    });

    long notZeroSpeedCount = 0;
    long msStart = System.currentTimeMillis();
    long counter = 0;

    while (notZeroSpeedCount < 1000) {
      if (counter % 1000 == 0 && System.currentTimeMillis() - msStart > 10000)
        break;

      speedData = getDataFunction.getData(storagePerformanceStatistic);

      //background thread did not run.
      if (speedData[0] == -1) {
        Thread.yield();
      } else {
        notZeroSpeedCount++;
        assertDataFunction.assertData(speedData);
      }
      Thread.yield();

      counter++;
    }

    Assert.assertTrue(notZeroSpeedCount > 0);
    System.out.println(testName + " : not zero speed count " + notZeroSpeedCount);

    storagePerformanceStatistic.stopMeasurement();
    measurementStopped.set(true);

    speedData = getDataFunction.getData(storagePerformanceStatistic);
    if (speedData[0] > 0) {
      System.out.println(testName + " : check that values were not changed after stop measurement");
      nanoIncrement.set(10);
      Thread.sleep(200);

      assertDataFunction.assertData(getDataFunction.getData(storagePerformanceStatistic));
      Assert.assertFalse(storagePerformanceStatistic.isMeasurementEnabled());
    }

    stop.set(true);

    future.get();

    storagePerformanceStatistic.startMeasurement();
    measurementStopped.set(false);

    Thread.sleep(200);

    Assert.assertTrue(storagePerformanceStatistic.isMeasurementEnabled());
    speedData = getDataFunction.getData(storagePerformanceStatistic);
    for (long datum : speedData) {
      Assert.assertEquals(datum, -1);
    }

    executor.shutdown();
  }

  private void testMBeanCase(String testName, final LoadLoop loadLoop, final GetDataFunction getDataFunction,
      final AssertDataFunction assertDataFunction) throws Exception {
    final AtomicLong nanoIncrement = new AtomicLong();
    nanoIncrement.set(100);

    final OStoragePerformanceStatistic storagePerformanceStatistic = new OStoragePerformanceStatistic(1024, "test", 1,
        new OStoragePerformanceStatistic.NanoTimer() {
          private long counter = 0;

          @Override
          public long getNano() {
            return counter += nanoIncrement.get();
          }
        }, 100000); // take measurements during 100 ms

    storagePerformanceStatistic.registerMBean();

    final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    final ObjectName objectName = new ObjectName("com.orientechnologies.orient.core.storage.impl.local."
        + "statistic:type=OStoragePerformanceStatisticMXBean,name=test,id=1");

    Assert.assertTrue(mbs.isRegistered(objectName));

    storagePerformanceStatistic.registerMBean();

    final OStoragePerformanceStatisticMXBean mxBean = MBeanServerInvocationHandler
        .newProxyInstance(mbs, objectName, OStoragePerformanceStatisticMXBean.class, false);

    Assert.assertFalse(mxBean.isMeasurementEnabled());

    long[] speedData = getDataFunction.getData(mxBean);
    for (long datum : speedData) {
      Assert.assertEquals(datum, -1);
    }

    mxBean.startMeasurement();

    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicBoolean measurementStopped = new AtomicBoolean();
    final ExecutorService executor = Executors.newSingleThreadExecutor();

    final Future<Void> future = executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        while (!stop.get()) {
          loadLoop.execute(storagePerformanceStatistic, measurementStopped.get());
        }
        return null;
      }
    });

    long notZeroSpeedCount = 0;
    long msStart = System.currentTimeMillis();
    long counter = 0;

    while (notZeroSpeedCount < 1000) {
      if (counter % 1000 == 0 && System.currentTimeMillis() - msStart > 10000)
        break;

      speedData = getDataFunction.getData(mxBean);

      //background thread did not run.
      if (speedData[0] == -1) {
        Thread.yield();
      } else {
        notZeroSpeedCount++;
        assertDataFunction.assertData(speedData);
      }
      Thread.yield();

      counter++;
    }

    Assert.assertTrue(notZeroSpeedCount > 0);
    System.out.println(testName + "MBean : not zero speed count " + notZeroSpeedCount);

    mxBean.stopMeasurement();
    measurementStopped.set(true);

    speedData = getDataFunction.getData(mxBean);
    if (speedData[0] > 0) {
      System.out.println(testName + "MBean : check that values were not changed after stop measurement");
      nanoIncrement.set(10);
      Thread.sleep(200);

      assertDataFunction.assertData(getDataFunction.getData(mxBean));
      Assert.assertFalse(mxBean.isMeasurementEnabled());
    }

    stop.set(true);

    future.get();

    mxBean.startMeasurement();
    measurementStopped.set(false);

    Thread.sleep(200);

    Assert.assertTrue(mxBean.isMeasurementEnabled());

    speedData = getDataFunction.getData(mxBean);

    for (long datum : speedData) {
      Assert.assertEquals(datum, -1);
    }

    executor.shutdown();

    storagePerformanceStatistic.unregisterMBean();
    Assert.assertFalse(mbs.isRegistered(objectName));

    storagePerformanceStatistic.unregisterMBean();
  }

  private interface LoadLoop {
    void execute(OStoragePerformanceStatistic storagePerformanceStatistic, boolean measurementStopped);
  }

  private interface GetDataFunction {
    long[] getData(OStoragePerformanceStatisticMXBean storagePerformanceStatistic);
  }

  private interface AssertDataFunction {
    void assertData(long[] data) throws AssertionError;
  }

}
