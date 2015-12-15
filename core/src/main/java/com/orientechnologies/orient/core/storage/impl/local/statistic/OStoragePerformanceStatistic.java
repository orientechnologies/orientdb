package com.orientechnologies.orient.core.storage.impl.local.statistic;

import com.orientechnologies.common.concur.lock.ODistributedCounter;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.OReadCacheException;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tool which allows to gather statistic about storage performance and expose it as MBean to the users.
 * <p>
 * All statistic is started to be gathered in runtime when method {@link #startMeasurement()} is called and is stopped to be gathered
 * once {@link #stopMeasurement()} will be called. This method may be called from Java or from JMX client.
 * <p>
 * Statistic is gathered in runtime but it's values are updated once per second and exposed using getXXX methods.
 */
public class OStoragePerformanceStatistic implements OStoragePerformanceStatisticMXBean {
  /**
   * Timer which is used to update exposed statistic values (so called snapshots).
   */
  private final Timer updateTimer;

  /**
   * Task which is used to update exposed statistic values (aka snapshots).
   */
  private volatile TimerTask updateTask;

  /**
   * Amount of bytes in megabyte.
   */
  private static final int MEGABYTE = 1024 * 1024;

  /**
   * Amount of milliseconds in one second.
   */
  private static final int MILLISECONDS_IN_SECOND = 1000;

  /**
   * Amount of nanoseconds in second.
   */
  private static final int NANOS_IN_SECOND = 1000000000;

  /**
   * In order to speed up interaction in multithreading environment we use thread local stack of timestamps
   * which is used in start/stopXXXTimer methods.
   */
  private final ThreadLocal<Deque<Long>> tlTimeStumps = new ThreadLocal<Deque<Long>>() {
    @Override
    protected Deque<Long> initialValue() {
      return new ArrayDeque<Long>();
    }
  };

  /**
   * Flag which indicates whether measurement of storage performance statistic is enabled.
   */
  private volatile boolean measurementIsEnabled = false;

  /**
   * Amount of times when cache was accessed during the measurement session.
   */
  private final ODistributedCounter cacheAccessCount = new ODistributedCounter();

  /**
   * Snapshot of {@link #cacheAccessCount} value at the end of measurement session.
   */
  private volatile long cacheAccessCountSnapshot = 0;

  /**
   * Amount of "cache hit" times during the measurement session.
   */
  private final ODistributedCounter cacheHit = new ODistributedCounter();

  /**
   * Snapshot of {@link #cacheHit} value at the end of measurement session.
   */
  private volatile long cacheHitSnapshot = 0;

  /**
   * Summary time which was spent on access of pages from file system during the measurement session.
   */
  private final ODistributedCounter pageReadFromFileTime = new ODistributedCounter();

  /**
   * Snapshot of {@link #pageReadFromFileTime} value at the end of measurement session.
   */
  private volatile long pageReadFromFileTimeSnapshot = 0;

  /**
   * Amount of pages in total which were accessed from file system during the measurement session.
   */
  private final ODistributedCounter pageReadFromFileCount = new ODistributedCounter();

  /**
   * Snapshot of {@link #pageReadFromFileCount} value at the end of measurement session.
   */
  private volatile long pageReadFromFileCountSnapshot = 0;

  /**
   * Summary time which was spent on access of pages from disk cache during the measurement session.
   */
  private final ODistributedCounter pageReadFromCacheTime = new ODistributedCounter();

  /**
   * Snapshot of {@link #pageReadFromCacheTime} value at the end of measurement session.
   */
  private volatile long pageReadFromCacheTimeSnapshot = 0;

  /**
   * Amount of pages in total which were accessed from disk cache during the measurement session.
   */
  private final ODistributedCounter pageReadFromCacheCount = new ODistributedCounter();

  /**
   * Snapshot of {@link #pageReadFromCacheCount} value at the end of measurement session.
   */
  private volatile long pageReadFromCacheCountSnapshot = 0;

  /**
   * Summary time which was spent to write pages to disk cache during the measurement session.
   */
  private final ODistributedCounter pageWriteToCacheTime = new ODistributedCounter();

  /**
   * Snapshot of {@link #pageWriteToCacheTime} value at the end of measurement session.
   */
  private volatile long pageWriteToCacheTimeSnapshot = 0;

  /**
   * Amount of pages in total which were written to disk cache during the measurement session.
   */
  private final ODistributedCounter pageWriteToCacheCount = new ODistributedCounter();

  /**
   * Snapshot of {@link #pageWriteToCacheCount} value at the end of measurement session.
   */
  private volatile long pageWriteToCacheCountSnapshot = 0;

  /**
   * Summary time which was spent to write pages to file system during the measurement session.
   */
  private final ODistributedCounter pageWriteToFileTime = new ODistributedCounter();

  /**
   * Snapshot of {@link #pageWriteToFileTime} value at the end of measurement session.
   */
  private volatile long pageWriteToFileTimeSnapshot = 0;

  /**
   * Amount of pages in total which were written to file system during the measurement session.
   */
  private final ODistributedCounter pageWriteToFileCount = new ODistributedCounter();

  /**
   * Snapshot of {@link #pageWriteToFileCount} value at the end of measurement session.
   */
  private volatile long pageWriteToFileCountSnapshot = 0;

  /**
   * Amount of times when atomic operation commit was performed during the measurement session.
   */
  private final ODistributedCounter commitCount = new ODistributedCounter();

  /**
   * Snapshot of {@link #commitCount} value at the end of measurement session.
   */
  private volatile long commitCountSnapshot = 0;

  /**
   * Summary time which was spent on atomic operation commits during the measurement session.
   */
  private final ODistributedCounter commitTime = new ODistributedCounter();

  /**
   * Snapshot of {@link #commitTime} value at the end of measurement session.
   */
  private volatile long commitTimeSnapshot = 0;

  /**
   * Page size in cache.
   */
  private final int pageSize;

  /**
   * Name of MBean which is exposed to read performance statistic data and switch on/off statistics gathering.
   */
  private final String mbeanName;

  /**
   * Flag which indicates whether MBean is registered.
   */
  private final AtomicBoolean mbeanIsRegistered = new AtomicBoolean();

  /**
   * Creates object and initiates it with value of size of page in cache and set name of MBean exposed to JVM.
   *
   * @param pageSize    Page size in cache.
   * @param storageName Name of storage performance statistic of which is gathered.
   */
  public OStoragePerformanceStatistic(int pageSize, String storageName) {
    this.pageSize = pageSize;

    updateTimer = new Timer("Updater for storage performance statistic of " + storageName, true);
    mbeanName = "com.orientechnologies.orient.core.storage.impl.local.statistic:type=OStoragePerformanceStatisticMXBean,name="
        + storageName;
  }

  /**
   * Starts gathering of performance statistic for storage.
   */
  @Override
  public void startMeasurement() {
    measurementIsEnabled = true;

    cacheAccessCount.clear();
    cacheHit.clear();
    pageReadFromFileTime.clear();
    pageReadFromFileCount.clear();
    pageReadFromCacheTime.clear();
    pageReadFromCacheCount.clear();
    pageWriteToCacheTime.clear();
    pageWriteToCacheCount.clear();
    pageWriteToFileTime.clear();
    pageWriteToFileCount.clear();
    commitCount.clear();
    commitTime.clear();

    updateTask = new TimerTask() {
      @Override
      public void run() {
        cacheAccessCountSnapshot = cacheAccessCount.get();
        cacheHitSnapshot = cacheHit.get();
        pageReadFromFileTimeSnapshot = pageReadFromFileTime.get();
        pageReadFromFileCountSnapshot = pageReadFromFileCount.get();
        pageReadFromCacheTimeSnapshot = pageReadFromCacheTime.get();
        pageReadFromCacheCountSnapshot = pageReadFromCacheCount.get();
        pageWriteToCacheTimeSnapshot = pageWriteToCacheTime.get();
        pageWriteToCacheCountSnapshot = pageWriteToCacheCount.get();
        pageWriteToFileTimeSnapshot = pageWriteToFileTime.get();
        pageWriteToFileCountSnapshot = pageWriteToFileCount.get();
        commitCountSnapshot = commitCount.get();
        commitTimeSnapshot = commitTime.get();
      }
    };

    updateTimer.schedule(updateTask, MILLISECONDS_IN_SECOND, MILLISECONDS_IN_SECOND);
  }

  /**
   * Stops gathering of performance statistic for storage, but does not clear snapshot values.
   */
  @Override
  public void stopMeasurement() {
    final TimerTask ct = updateTask;
    if (ct != null) {
      ct.cancel();
    }

    measurementIsEnabled = false;
    updateTask = null;
  }

  /**
   * Registers MBean in MBean server.
   */
  public void registerMBean() {
    if (mbeanIsRegistered.compareAndSet(false, true)) {
      try {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final ObjectName beanName = new ObjectName(mbeanName);

        if (!server.isRegistered(beanName))
          server.registerMBean(this, beanName);

      } catch (MalformedObjectNameException e) {
        throw OException.wrapException(new OReadCacheException("Error during registration of read cache MBean"), e);
      } catch (InstanceAlreadyExistsException e) {
        throw OException.wrapException(new OReadCacheException("Error during registration of read cache MBean"), e);
      } catch (MBeanRegistrationException e) {
        throw OException.wrapException(new OReadCacheException("Error during registration of read cache MBean"), e);
      } catch (NotCompliantMBeanException e) {
        throw OException.wrapException(new OReadCacheException("Error during registration of read cache MBean"), e);
      }
    }
  }

  /**
   * Unregisters MBean from MBean server.
   */
  public void unregisterMBean() {
    if (mbeanIsRegistered.compareAndSet(true, false)) {
      try {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final ObjectName beanName = new ObjectName(mbeanName);

        if (server.isRegistered(beanName))
          server.unregisterMBean(beanName);

      } catch (MalformedObjectNameException e) {
        throw OException.wrapException(new OReadCacheException("Error during unregistration of read cache MBean"), e);
      } catch (InstanceNotFoundException e) {
        throw OException.wrapException(new OReadCacheException("Error during unregistration of read cache MBean"), e);
      } catch (MBeanRegistrationException e) {
        throw OException.wrapException(new OReadCacheException("Error during unregistration of read cache MBean"), e);
      }
    }
  }

  /**
   * @return Read speed of data in megabytes per second on cache level
   * or value which is less than 0, which means that value can not be calculated.
   */
  @Override
  public long getReadSpeedFromCacheInMB() {
    return (getReadSpeedFromCacheInPages() * pageSize) / MEGABYTE;
  }

  /**
   * @return Read speed of data in pages per second on cache level
   * or value which is less than 0, which means that value can not be calculated.
   */
  @Override
  public long getReadSpeedFromCacheInPages() {
    final long time = pageReadFromCacheTimeSnapshot;
    if (time == 0)
      return -1;

    return (pageReadFromCacheCountSnapshot * NANOS_IN_SECOND) / time;
  }

  /**
   * @return Read speed of data on file system level in pages per second
   * or value which is less than 0, which means that value can not be calculated.
   */
  @Override
  public long getReadSpeedFromFileInPages() {
    final long time = pageReadFromFileTimeSnapshot;
    if (time == 0)
      return -1;

    return (pageReadFromFileCountSnapshot * NANOS_IN_SECOND) / time;
  }

  /**
   * @return Read speed of data on file system level in megabytes per second
   * or value which is less than 0, which means that value can not be calculated.
   */
  @Override
  public long getReadSpeedFromFileInMB() {
    return (getReadSpeedFromFileInPages() * pageSize) / MEGABYTE;
  }

  /**
   * @return Amount of pages read from cache in total.
   */
  @Override
  public long getAmountOfPagesReadFromCache() {
    return pageReadFromCacheCountSnapshot;
  }

  /**
   * @return Amount of pages are read from file.
   */
  @Override
  public long getAmountOfPagesReadFromFileSystem() {
    return pageReadFromFileCountSnapshot;
  }

  /**
   * @return Write speed of data in pages per second on cache level
   * or value which is less than 0, which means that value can not be calculated.
   */
  @Override
  public long getWriteSpeedInCacheInPages() {
    final long time = pageWriteToCacheTimeSnapshot;
    if (time == 0)
      return -1;

    return (pageWriteToCacheCountSnapshot * NANOS_IN_SECOND) / time;
  }

  /**
   * @return Write speed of data in megabytes per second on cache level
   * or value which is less than 0, which means that value can not be calculated.
   */
  @Override
  public long getWriteSpeedInCacheInMB() {
    return (getWriteSpeedInCacheInPages() * pageSize) / MEGABYTE;
  }

  /**
   * @return Write speed of data in pages per second to file system
   * or value which is less than 0, which means that value can not be calculated.
   */
  @Override
  public long getWriteSpeedInFileInPages() {
    final long time = pageWriteToFileTimeSnapshot;
    if (time == 0)
      return -1;

    return (pageWriteToFileCountSnapshot * NANOS_IN_SECOND) / time;
  }

  /**
   * @return Write speed of data in megabytes per second to file system
   * or value which is less than 0, which means that value can not be calculated.
   */
  @Override
  public long getWriteSpeedInFileInMB() {
    return (getWriteSpeedInFileInPages() * pageSize) / MEGABYTE;
  }

  /**
   * @return Amount of pages written to cache.
   */
  @Override
  public long getAmountOfPagesWrittenToCache() {
    return pageWriteToCacheCountSnapshot;
  }

  /**
   * @return Average time of commit of atomic operation in nanoseconds
   * or value which is less than 0, which means that value can not be calculated.
   */
  @Override
  public long getCommitTimeAvg() {
    final long count = commitCountSnapshot;
    if (count == 0)
      return -1;

    return commitTimeSnapshot / count;
  }

  /**
   * @return Percent of cache hits
   * or value which is less than 0, which means that value can not be calculated.
   */
  @Override
  public int getCacheHits() {
    final long count = cacheAccessCountSnapshot;
    if (count == 0)
      return -1;

    return (int) ((cacheHitSnapshot * 100) / count);
  }

  /**
   * Starts timer which counts how much time was spent on read of page from file system.
   */
  public void startPageReadFromFileTimer() {
    pushTimeStamp();
  }

  /**
   * Put current value of nano time into thread local stack of timestamps {@link #tlTimeStumps}.
   * That is utility method which is used by all startXXXTimer methods.
   */
  private void pushTimeStamp() {
    if (measurementIsEnabled) {
      final Deque<Long> stamps = tlTimeStumps.get();
      stamps.push(System.nanoTime());
    }
  }

  /**
   * Stops and records results of timer which counts how much time was spent on read of page from file system.
   *
   * @param readPages Amount of pages which were read by single call to file system.
   */
  public void stopPageReadFromFileTimer(int readPages) {
    final Deque<Long> stamps = tlTimeStumps.get();
    if (stamps.size() > 0) {
      final long endTs = System.nanoTime();

      pageReadFromFileTime.add(endTs - stamps.pop());
      pageReadFromFileCount.add(readPages);
    }
  }

  /**
   * Starts timer which counts how much time was spent on read of page from disk cache.
   */
  public void startPageReadFromCacheTimer() {
    pushTimeStamp();
  }

  /**
   * Stops and records results of timer which counts how much time was spent on read of page from disk cache.
   */
  public void stopPageReadFromCacheTimer() {
    final Deque<Long> stamps = tlTimeStumps.get();
    if (stamps.size() > 0) {
      final long endTs = System.nanoTime();

      pageReadFromCacheTime.add(endTs - stamps.pop());
      pageReadFromCacheCount.increment();
    }
  }

  /**
   * Starts timer which counts how much time was spent on write of page to disk cache.
   */
  public void startPageWriteToCacheTimer() {
    pushTimeStamp();
  }

  /**
   * Stops and records results of timer which counts how much time was spent to write page to disk cache.
   */
  public void stopPageWriteToCacheTimer() {
    final Deque<Long> stamps = tlTimeStumps.get();
    if (stamps.size() > 0) {
      final long endTs = System.nanoTime();

      pageWriteToCacheTime.add(endTs - stamps.pop());
      pageWriteToCacheCount.increment();
    }
  }

  /**
   * Starts timer which counts how much time was spent on write of page to disk cache.
   */
  public void startPageWriteToFileTimer() {
    pushTimeStamp();
  }

  /**
   * Stops and records results of timer which counts how much time was spent to write page to disk cache.
   */
  public void stopPageWriteToFileTimer() {
    final Deque<Long> stamps = tlTimeStumps.get();
    if (stamps.size() > 0) {
      final long endTs = System.nanoTime();

      pageWriteToFileTime.add(endTs - stamps.pop());
      pageWriteToFileCount.increment();
    }
  }

  /**
   * Increments counter of page accesses from cache.
   */
  public void incrementPageAccessOnCacheLevel() {
    if (measurementIsEnabled) {
      cacheAccessCount.increment();
    }
  }

  /**
   * Increments counter of cache hits
   */
  public void incrementCacheHit() {
    if (measurementIsEnabled) {
      cacheHit.increment();
    }
  }

  /**
   * Starts timer which counts how much time was spent on atomic operation commit.
   */
  public void startCommitTimer() {
    pushTimeStamp();
  }

  /**
   * Stops and records results of timer which counts how much time was spent on atomic operation commit.
   */
  public void stopCommitTimer() {
    final Deque<Long> stamps = tlTimeStumps.get();
    if (stamps.size() > 0) {
      final long endTs = System.nanoTime();

      commitTime.add(endTs - stamps.pop());
      commitCount.increment();
    }
  }
}
