package com.orientechnologies.orient.core.storage.impl.local.statistic;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.OReadCacheException;
import com.orientechnologies.orient.core.storage.OIdentifiableStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tool which allows to gather statistic about storage performance and exposes it as MBean to the users.
 * <p>
 * All statistic is started to be gathered in runtime when method {@link #startMeasurement()} is called and is stopped to be gathered
 * once {@link #stopMeasurement()} will be called. This method may be called from Java or from JMX client.
 * <p>
 * Statistic is gathered in runtime but it's values are exposed to the user once in specified time interval (this interval is called
 * measurement session) (1 second by default) and are available using getXXX methods. Exposed values are called
 * snapshots. All data are gathered during the session,
 * then latest values are taken as snapshots and after that runtime values are cleared (but snapshots are kept) and measurement
 * of storage performance is started again.
 */
public class OStoragePerformanceStatistic implements OStoragePerformanceStatisticMXBean {
  /**
   * Timer which is used to update exposed statistic values (so called snapshots).
   */
  private final ScheduledExecutorService updateTimer;

  /**
   * Task which is used to update exposed statistic values (aka snapshots).
   */
  private volatile ScheduledFuture<?> updateTask;

  /**
   * Interval of time for single session of storage performance measurement in nano seconds (1s by default).
   */
  private final long snapshotInterval;

  /**
   * Amount of bytes in megabyte.
   */
  private static final int MEGABYTE = 1024 * 1024;

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
   * Container for all performance counters are used during measurement of storage performance.
   */
  private final AtomicReference<PerformanceCounters> performanceCounters = new AtomicReference<PerformanceCounters>();

  /**
   * Snapshot of amount of cache hits calculated from  {@link PerformanceCounters#cacheHits}
   * value at the end of measurement session.
   */
  private volatile int cacheHitsSnapshot = -1;

  /**
   * Snapshot of read speed of pages per second from file system calculated from
   * {@link PerformanceCounters#readSpeedFromFile} value at the end of measurement session.
   */
  private volatile long readSpeedFromFileSnapshot = -1;

  /**
   * Snapshot of read speed of pages per second from disk cache calculated from
   * {@link PerformanceCounters#readSpeedFromCache} value at the end of measurement session.
   */
  private volatile long readSpeedFromCacheSnapshot = -1;

  /**
   * Snapshot of write speed of pages in disk cache calculated from
   * {@link PerformanceCounters#writeSpeedInCache} value at the end of measurement session.
   */
  private volatile long writeSpeedInCacheSnapshot = -1;

  /**
   * Snapshot of write speed of pages in file calculated from
   * {@link PerformanceCounters#writeSpeedInFile} value at the end of measurement session.
   */
  private volatile long writeSpeedInFileSnapshot = -1;

  /**
   * Snapshot of average time of atomic operation commit calculated from
   * {@link PerformanceCounters#commitTimeAvg} value at the end of measurement session.
   */
  private volatile long commitTimeAvgSnapshot = -1;

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
   * Object which is used to get current PC nano time.
   */
  private final NanoTimer nanoTimer;

  /**
   * Creates object and initiates it with value of size of page in cache and set name of MBean exposed to JVM.
   *
   * @param pageSize    Page size in cache.
   * @param storageName Name of storage performance statistic of which is gathered.
   * @param storageId   Id of storage {@link OIdentifiableStorage#getId()}
   */
  public OStoragePerformanceStatistic(int pageSize, String storageName, long storageId) {
    this(pageSize, storageName, storageId, new NanoTimer() {
      @Override
      public long getNano() {
        return System.nanoTime();
      }
    }, NANOS_IN_SECOND);
  }

  /**
   * Creates object and initiates it with value of size of page in cache and set name of MBean exposed to JVM.
   *
   * @param pageSize         Page size in cache.
   * @param storageName      Name of storage performance statistic of which is gathered.
   * @param storageId        Id of storage {@link OIdentifiableStorage#getId()}
   * @param nanoTimer        Object which is used to get current PC nano time.
   * @param snapshotInterval How much session management lasts.
   */
  public OStoragePerformanceStatistic(int pageSize, String storageName, long storageId, NanoTimer nanoTimer,
      long snapshotInterval) {
    this.pageSize = pageSize;

    updateTimer = Executors.newSingleThreadScheduledExecutor(new SnapshotTaskFactory(storageName));

    mbeanName =
        "com.orientechnologies.orient.core.storage.impl.local.statistic:type=OStoragePerformanceStatisticMXBean,name=" + storageName
            + ",id=" + storageId;

    this.nanoTimer = nanoTimer;
    this.snapshotInterval = snapshotInterval;
  }

  /**
   * Starts gathering of performance statistic for storage.
   */
  @Override
  public void startMeasurement() {
    performanceCounters.set(new PerformanceCounters());

    updateTask = updateTimer.scheduleWithFixedDelay(new SnapshotTask(), snapshotInterval, snapshotInterval, TimeUnit.NANOSECONDS);
  }

  /**
   * Stops gathering of performance statistic for storage, but does not clear snapshot values.
   */
  @Override
  public void stopMeasurement() {
    final ScheduledFuture<?> ct = updateTask;
    if (ct != null) {
      ct.cancel(false);
    }

    performanceCounters.set(null);
    updateTask = null;
  }

  /**
   * @return <code>true</code> if statistic is measured inside of storage.
   */
  @Override
  public boolean isMeasurementEnabled() {
    return performanceCounters.get() != null;
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
    final long pagesSpeed = getReadSpeedFromCacheInPages();
    if (pagesSpeed < 0)
      return -1;

    return (pagesSpeed * pageSize) / MEGABYTE;
  }

  /**
   * @return Read speed of data in pages per second on cache level
   * or value which is less than 0, which means that value can not be calculated.
   */
  @Override
  public long getReadSpeedFromCacheInPages() {
    return readSpeedFromCacheSnapshot;
  }

  /**
   * @return Read speed of data on file system level in pages per second
   * or value which is less than 0, which means that value can not be calculated.
   */
  @Override
  public long getReadSpeedFromFileInPages() {
    return readSpeedFromFileSnapshot;
  }

  /**
   * @return Read speed of data on file system level in megabytes per second
   * or value which is less than 0, which means that value can not be calculated.
   */
  @Override
  public long getReadSpeedFromFileInMB() {
    final long pageSpeed = getReadSpeedFromFileInPages();
    if (pageSpeed < 0)
      return -1;

    return (pageSpeed * pageSize) / MEGABYTE;
  }

  /**
   * @return Write speed of data in pages per second on cache level
   * or value which is less than 0, which means that value can not be calculated.
   */
  @Override
  public long getWriteSpeedInCacheInPages() {
    return writeSpeedInCacheSnapshot;
  }

  /**
   * @return Write speed of data in megabytes per second on cache level
   * or value which is less than 0, which means that value can not be calculated.
   */
  @Override
  public long getWriteSpeedInCacheInMB() {
    final long pageSpeed = getWriteSpeedInCacheInPages();
    if (pageSpeed < 0)
      return -1;

    return (pageSpeed * pageSize) / MEGABYTE;
  }

  /**
   * @return Write speed of data in pages per second to file system
   * or value which is less than 0, which means that value can not be calculated.
   */
  @Override
  public long getWriteSpeedInFileInPages() {
    return writeSpeedInFileSnapshot;
  }

  /**
   * @return Write speed of data in megabytes per second to file system
   * or value which is less than 0, which means that value can not be calculated.
   */
  @Override
  public long getWriteSpeedInFileInMB() {
    final long pageSpeed = getWriteSpeedInFileInPages();
    if (pageSpeed < 0)
      return -1;

    return (pageSpeed * pageSize) / MEGABYTE;
  }

  /**
   * @return Average time of commit of atomic operation in nanoseconds
   * or value which is less than 0, which means that value can not be calculated.
   */
  @Override
  public long getCommitTimeAvg() {
    return commitTimeAvgSnapshot;
  }

  /**
   * @return Percent of cache hits
   * or value which is less than 0, which means that value can not be calculated.
   */
  @Override
  public int getCacheHits() {
    return cacheHitsSnapshot;
  }

  /**
   * Starts timer which counts how much time was spent on read of page from file system.
   */
  public void startPageReadFromFileTimer() {
    pushTimeStamp();
  }

  /**
   * Push current value of nano time into thread local stack of timestamps {@link #tlTimeStumps}.
   * That is utility method which is used by all startXXXTimer methods.
   */
  private void pushTimeStamp() {
    //we push timestamps only if measurement is started, which is indicated by presence
    //of performance counters
    if (performanceCounters.get() != null) {
      final Deque<Long> stamps = tlTimeStumps.get();
      stamps.push(nanoTimer.getNano());
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
      final long endTs = nanoTimer.getNano();
      final long timeDiff = endTs - stamps.pop();

      final PerformanceCounters pf = performanceCounters.get();
      //we do not do null check at the beginning to do not allow to have dangling time stamps
      //in thread local stack
      if (pf != null) {
        final AtomicReference<OTimeCounter> readSpeedFromFile = pf.readSpeedFromFile;

        OTimeCounter oldReadSpeedFromFile = readSpeedFromFile.get();
        OTimeCounter newReadSpeedFromFile = new OTimeCounter(oldReadSpeedFromFile.getTime() + timeDiff,
            oldReadSpeedFromFile.getCounter() + readPages);

        while (!readSpeedFromFile.compareAndSet(oldReadSpeedFromFile, newReadSpeedFromFile)) {
          oldReadSpeedFromFile = readSpeedFromFile.get();
          newReadSpeedFromFile = new OTimeCounter(oldReadSpeedFromFile.getTime() + timeDiff,
              oldReadSpeedFromFile.getCounter() + readPages);
        }
      }
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
      final long endTs = nanoTimer.getNano();
      final long timeDiff = endTs - stamps.pop();

      final PerformanceCounters pf = performanceCounters.get();
      //we do not make null check at the begging because we do not want to have
      //dangling time stamps at the thread local stack
      if (pf != null) {
        final AtomicReference<OTimeCounter> readSpeedFromCache = pf.readSpeedFromCache;
        OTimeCounter oldReadSpeedFromCache = readSpeedFromCache.get();
        OTimeCounter newReadSpeedFromCache = new OTimeCounter(oldReadSpeedFromCache.getTime() + timeDiff,
            oldReadSpeedFromCache.getCounter() + 1);

        while (!readSpeedFromCache.compareAndSet(oldReadSpeedFromCache, newReadSpeedFromCache)) {
          oldReadSpeedFromCache = readSpeedFromCache.get();
          newReadSpeedFromCache = new OTimeCounter(oldReadSpeedFromCache.getTime() + timeDiff,
              oldReadSpeedFromCache.getCounter() + 1);
        }
      }
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
      final long endTs = nanoTimer.getNano();
      final long timeDiff = endTs - stamps.pop();

      final PerformanceCounters pf = performanceCounters.get();
      //we do not make null check at the begging because we do not want to have
      //dangling time stamps at the thread local stack
      if (pf != null) {
        final AtomicReference<OTimeCounter> writeSpeedInCache = pf.writeSpeedInCache;
        OTimeCounter oldWriteSpeedInCache = writeSpeedInCache.get();
        OTimeCounter newWriteSpeedInCache = new OTimeCounter(oldWriteSpeedInCache.getTime() + timeDiff,
            oldWriteSpeedInCache.getCounter() + 1);
        while (!writeSpeedInCache.compareAndSet(oldWriteSpeedInCache, newWriteSpeedInCache)) {
          oldWriteSpeedInCache = writeSpeedInCache.get();
          newWriteSpeedInCache = new OTimeCounter(oldWriteSpeedInCache.getTime() + timeDiff, oldWriteSpeedInCache.getCounter() + 1);
        }
      }
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
      final long endTs = nanoTimer.getNano();
      final long timeDiff = endTs - stamps.pop();

      final PerformanceCounters pf = performanceCounters.get();
      //we do not make null check at the begging because we do not want to have
      //dangling time stamps at the thread local stack
      if (pf != null) {
        final AtomicReference<OTimeCounter> writeSpeedInFile = pf.writeSpeedInFile;
        OTimeCounter oldWriteSpeedInFile = writeSpeedInFile.get();
        OTimeCounter newWriteSpeedInFile = new OTimeCounter(oldWriteSpeedInFile.getTime() + timeDiff,
            oldWriteSpeedInFile.getCounter() + 1);
        while (!writeSpeedInFile.compareAndSet(oldWriteSpeedInFile, newWriteSpeedInFile)) {
          oldWriteSpeedInFile = writeSpeedInFile.get();
          newWriteSpeedInFile = new OTimeCounter(oldWriteSpeedInFile.getTime() + timeDiff, oldWriteSpeedInFile.getCounter() + 1);
        }
      }
    }
  }

  /**
   * Increments counter of page accesses from cache.
   *
   * @param cacheHit Indicates that accessed page was found in cache.
   */
  public void incrementPageAccessOnCacheLevel(boolean cacheHit) {
    final PerformanceCounters pf = performanceCounters.get();
    if (pf != null) {
      final AtomicReference<OTimeCounter> cacheHits = pf.cacheHits;

      OTimeCounter oldCacheHits = cacheHits.get();
      OTimeCounter newCacheHits = new OTimeCounter(oldCacheHits.getTime() + (cacheHit ? 1 : 0), oldCacheHits.getCounter() + 1);

      while (!cacheHits.compareAndSet(oldCacheHits, newCacheHits)) {
        oldCacheHits = cacheHits.get();
        newCacheHits = new OTimeCounter(oldCacheHits.getTime() + (cacheHit ? 1 : 0), oldCacheHits.getCounter() + 1);
      }
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
      final long endTs = nanoTimer.getNano();
      final long timeDiff = endTs - stamps.pop();

      final PerformanceCounters pf = performanceCounters.get();
      //we do not make null check at the begging because we do not want to have
      //dangling time stamps at the thread local stack
      if (pf != null) {
        final AtomicReference<OTimeCounter> commitTimeAvg = pf.commitTimeAvg;
        OTimeCounter oldCommitTimeAvg = commitTimeAvg.get();
        OTimeCounter newCommitTimeAvg = new OTimeCounter(oldCommitTimeAvg.getTime() + timeDiff, oldCommitTimeAvg.getCounter() + 1);
        while (!commitTimeAvg.compareAndSet(oldCommitTimeAvg, newCommitTimeAvg)) {
          oldCommitTimeAvg = commitTimeAvg.get();
          newCommitTimeAvg = new OTimeCounter(oldCommitTimeAvg.getTime() + timeDiff, oldCommitTimeAvg.getCounter() + 1);
        }
      }
    }
  }

  /**
   * Interface which is used by this tool to get current PC nano time.
   * Implementation which calls <code>System.nanoTime()</code> is used by default.
   */
  public interface NanoTimer {
    /**
     * @return Current PC nano time.
     */
    long getNano();
  }

  /**
   * Task to move gathered values about performance statistic at the end of measurement session to snapshot holders.
   */
  private final class SnapshotTask implements Runnable {

    /**
     * Moves counters values to snapshot values.
     */
    @Override
    public void run() {
      final PerformanceCounters pf = performanceCounters.get();
      //we make snapshots only if performance measurement is enabled, so counters holder is not null
      if (pf != null) {
        //clear counters, CAS may be unsuccessful if we stopped measurement of
        //storage performance in this case counters holder will be set to null
        //or in case if we start new measurement session (holder instance will be changed in such case)
        //and we do not need to clear it yet.
        performanceCounters.compareAndSet(pf, new PerformanceCounters());

        final OTimeCounter cacheHits = pf.cacheHits.get();

        //unusual case where cache hits is time and total access count is counter.
        if (cacheHits.getCounter() == 0)
          cacheHitsSnapshot = -1;
        else
          cacheHitsSnapshot = (int) ((cacheHits.getTime() * 100) / cacheHits.getCounter());

        //calculate speeds per second
        readSpeedFromFileSnapshot = pf.readSpeedFromFile.get().calculateSpeedPerTimeInter(NANOS_IN_SECOND);
        readSpeedFromCacheSnapshot = pf.readSpeedFromCache.get().calculateSpeedPerTimeInter(NANOS_IN_SECOND);
        writeSpeedInCacheSnapshot = pf.writeSpeedInCache.get().calculateSpeedPerTimeInter(NANOS_IN_SECOND);
        writeSpeedInFileSnapshot = pf.writeSpeedInFile.get().calculateSpeedPerTimeInter(NANOS_IN_SECOND);

        commitTimeAvgSnapshot = pf.commitTimeAvg.get().calculateAvgTime();
      }
    }
  }

  /**
   * Thread factory for {@link SnapshotTask}
   */
  private final static class SnapshotTaskFactory implements ThreadFactory {
    private final String storageName;

    public SnapshotTaskFactory(String storageName) {
      this.storageName = storageName;
    }

    @Override
    public Thread newThread(Runnable r) {
      final Thread thread = new Thread(OAbstractPaginatedStorage.storageThreadGroup, r);
      thread.setName("Updater for storage performance statistic of " + storageName);
      thread.setDaemon(true);

      return thread;
    }
  }

  /**
   * Holder for performance counters of storage statistic, is used to:
   * <p>
   * <ol>
   * <li>Indicate whether measurement of storage performance is switched on/off</li>
   * <li>Speed up cleaning of performance counters at the end of measurement session</li>
   * </ol>
   */
  private static final class PerformanceCounters {
    /**
     * Amount of times when cache was accessed during the measurement session and
     * amount of "cache hit" times during the measurement session.
     */
    private final AtomicReference<OTimeCounter> cacheHits = new AtomicReference<OTimeCounter>(new OTimeCounter(0, 0));

    /**
     * Summary time which was spent on access of pages from file system during the measurement session and
     * Amount of pages in total which were accessed from file system during the measurement session.
     */
    private final AtomicReference<OTimeCounter> readSpeedFromFile = new AtomicReference<OTimeCounter>(new OTimeCounter(0, 0));

    /**
     * Summary time which was spent on access of pages from disk cache during the measurement session
     * and amount of pages in total which were accessed from disk cache during the measurement session.
     */
    private final AtomicReference<OTimeCounter> readSpeedFromCache = new AtomicReference<OTimeCounter>(new OTimeCounter(0, 0));

    /**
     * Summary time which was spent to write pages to disk cache during the measurement session
     * and amount of pages in total which were written to disk cache during the measurement session.
     */
    private final AtomicReference<OTimeCounter> writeSpeedInCache = new AtomicReference<OTimeCounter>(new OTimeCounter(0, 0));

    /**
     * Summary time which was spent to write pages to file system during the measurement session
     * and amount of pages in total which were written to file system during the measurement session.
     */
    private final AtomicReference<OTimeCounter> writeSpeedInFile = new AtomicReference<OTimeCounter>(new OTimeCounter(0, 0));

    /**
     * Amount of times when atomic operation commit was performed during the measurement session
     * and summary time which was spent on atomic operation commits during the measurement session.
     */
    private final AtomicReference<OTimeCounter> commitTimeAvg = new AtomicReference<OTimeCounter>(new OTimeCounter(0, 0));
  }

}
