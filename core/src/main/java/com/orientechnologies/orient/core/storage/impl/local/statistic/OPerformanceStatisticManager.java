/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.core.storage.impl.local.statistic;

import com.orientechnologies.common.concur.lock.OReadersWriterSpinLock;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.OIdentifiableStorage;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.orientechnologies.orient.core.storage.impl.local.statistic.OSessionStoragePerformanceStatistic.*;

/**
 * Aggregator of performance statistic for whole storage. Allows to gather performance statistic for single thread or for all
 * threads in storage. In case of gathering statistics for single thread statistic for all period is gathered and average values are
 * provided. If you gather statistic for whole system time series of measurements are provided. You can not measure statistic for
 * whole system and for chosen threads at the same time. To gather statistic for single thread use following workflow:
 * <ol>
 * <li>Call {@link #startThreadMonitoring()}</li>
 * <li>Execute database commands</li>
 * <li>Call {@link #stopThreadMonitoring()}</li>
 * </ol>
 * Instance of {@link OSessionStoragePerformanceStatistic} returned as result of call of last method will contain all performance
 * data are gathered during storage monitoring. To gather statistic for whole system use following workflow:
 * <ol>
 * <li>Call {@link #startMonitoring()}</li>
 * <li>During monitoring of storage use getXXX methods to get information about performance numbers</li>
 * <li>At the end of monitoring call {@link #stopMonitoring()}</li>
 * </ol>
 * You may access performance data both after you stopped gathering statistic and during gathering of statistic. You may manipulate
 * by manager directly from Java or from JMX from bean with name which consist of prefix {@link #MBEAN_PREFIX} and storage name. If
 * {@link com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent} participates in performance
 * monitoring it has to register itself using method {@link #registerComponent(String)}
 */
public class OPerformanceStatisticManager {
  /**
   * Prefix of name of JMX bean.
   */
  private static final String MBEAN_PREFIX = "com.orientechnologies.orient.core.storage.impl.local.statistic:type=OPerformanceStatisticManagerMXBean";

  /*
   * Class consist of three key fields:
   * <p>
   * <ol>
   * <li>{@link #statistics}</li>
   * <li>{@link #deadThreadsStatistic}</li>
   * <li>{@link #postMeasurementStatistic}</li>
   * </ol>
   * <p>
   * When we call {@link #getSessionPerformanceStatistic()} we put current thread and related {@link OSessionStoragePerformanceStatistic} object
   * into {@link #statistics} map. Because some threads may die and new born during measurement of performance static each call to
   * getXXX method checks whether thread in {@link #statistics} map leave or dead. If dead threads are found they removed from map
   * and all statics from those threads is accumulated in {@link #deadThreadsStatistic} field.
   * <p>
   * At the end of measurement of performance data both fields {@link #statistics} and {@link #deadThreadsStatistic} are cleared and all
   * statistic is accumulated in {@link #postMeasurementStatistic} statistic. This field will be cleared during next call of
   * {@link #startMonitoring()} method.
   */

  /**
   * Interval between time series for each thread statistic.
   *
   * @see OSessionStoragePerformanceStatistic
   */
  private final long intervalBetweenSnapshots;

  /**
   * Interval between two time series.
   */
  private final long cleanUpInterval;

  /**
   * Lock for switching on/off statistic measurements.
   */
  private final OReadersWriterSpinLock switchLock = new OReadersWriterSpinLock();

  /**
   * Indicates whether gathering of performance data for whole system is switched on/off.
   */
  private volatile boolean enabled = false;

  /**
   * Indicates whether gathering of performance data for single thread is switched on/off.
   */
  private final ThreadLocal<Boolean> enabledForCurrentThread = ThreadLocal.withInitial(() -> false);

  /**
   * Map which bounds performance statistic for currently running threads.
   */
  private final ConcurrentHashMap<Thread, OSessionStoragePerformanceStatistic> statistics = new ConcurrentHashMap<>();

  /**
   * Accumulated statistic for threads which were active during early stages of performance measurement and are died now.
   */
  private volatile ImmutableStatistic deadThreadsStatistic;

  /**
   * Statistic accumulated after {@link #stopMonitoring()} method was called.
   */
  private volatile ImmutableStatistic postMeasurementStatistic;

  /**
   * Lock which ensures that only single thread may accumulate statistic from dead threads at the moment.
   */
  private final Lock deadThreadsUpdateLock = new ReentrantLock();

  /**
   * Indicates whether JMX bean for current object is already registered.
   */
  private final AtomicBoolean mbeanIsRegistered = new AtomicBoolean();

  /**
   * List of all components which registered itself as participating in performance monitoring.
   */
  private final List<String> componentNames = new CopyOnWriteArrayList<>();

  /**
   * Amount of full checkpoints are done for current storage. Value is updated on demand if monitoring is switched on. Supported
   * only for disk based storage.
   */
  private long fullCheckpointCount = -1;

  /**
   * Size of write ahead log in bytes. Value is updated on demand if monitoring is switched on. Supported only for disk based
   * storage.
   */
  private long walSize = -1;

  /**
   * Amount of times when WAL cache is completely filled in and force flush is needed. Value is updated on demand if monitoring is
   * switched on. Supported only for disk based storage.
   */
  private long walCacheOverflowCount = -1;

  /**
   * Size of read cache in bytes. Value is updated on demand if monitoring is switched on. Supported only for disk based storage.
   */
  private long readCacheSize = -1;

  /**
   * Size of write cache in bytes. Value is updated on demand if monitoring is switched on. Supported only for disk based storage.
   */
  private long writeCacheSize = -1;

  /**
   * Size of part of write cache which is not shared with read cache in bytes. Value is updated on demand if monitoring is switched
   * on. Supported only for disk based storage.
   */
  private long exclusiveWriteCacheSize = 1;

  /**
   * Amount of times when size of write cache is not enough to keep dirty pages and flush of write cache is forced. Value is updated
   * on demand if monitoring is switched on. Supported only for disk based storage.
   */
  private long writeCacheOverflowCount = -1;

  /**
   * Instance of storage to which performance manager belongs to.
   */
  private final OAbstractPaginatedStorage storage;

  /**
   * Flags which indicates whether {@link #wowCache} field is initialized on demand. We use separate flag because <code>null</code>
   * can be valid value.
   */
  private volatile boolean   wowCacheInitialized;
  /**
   * Instance of write cache which belongs to current storage, may be <code>null</code> if it is not disk based storage. Initialized
   * on demand.
   */
  private volatile OWOWCache wowCache;

  private volatile boolean readCacheInitialized;

  private volatile boolean writeAheadLogInitialized;

  /**
   * @param intervalBetweenSnapshots Interval between snapshots of performance data for each thread statistic.
   * @param cleanUpInterval          Interval between time series of performance data.
   * @see OSessionStoragePerformanceStatistic
   */
  public OPerformanceStatisticManager(OAbstractPaginatedStorage storage, long intervalBetweenSnapshots, long cleanUpInterval) {
    this.intervalBetweenSnapshots = intervalBetweenSnapshots;
    this.cleanUpInterval = cleanUpInterval;
    this.storage = storage;
  }

  /**
   * @return Returns current instance of write cache and initializes local reference if such one is not initialized yet.
   */
  private OWOWCache getWowCache() {
    if (wowCacheInitialized)
      return wowCache;

    final OWriteCache writeCache = storage.getWriteCache();
    if (writeCache instanceof OWOWCache)
      this.wowCache = (OWOWCache) writeCache;
    else
      this.wowCache = null;

    wowCacheInitialized = true;

    return this.wowCache;
  }

  /**
   * Starts performance monitoring only for single thread. After call of this method you can not start system wide monitoring till
   * call of {@link #stopThreadMonitoring()} is performed.
   */
  public void startThreadMonitoring() {
    switchLock.acquireWriteLock();
    try {
      if (enabled)
        throw new IllegalStateException("Monitoring is already started on system level and can not be started on thread level");

      enabledForCurrentThread.set(true);
      statistics.put(Thread.currentThread(), new OSessionStoragePerformanceStatistic(intervalBetweenSnapshots, Long.MAX_VALUE));
    } finally {
      switchLock.releaseWriteLock();
    }
  }

  /**
   * @return Container for gathered performance statistic or <code>null</code> if {@link #startThreadMonitoring()} is not called.
   */
  public OSessionStoragePerformanceStatistic stopThreadMonitoring() {
    switchLock.acquireWriteLock();
    try {
      enabledForCurrentThread.set(false);

      final Thread currentThread = Thread.currentThread();
      return statistics.remove(currentThread);
    } finally {
      switchLock.releaseWriteLock();
    }
  }

  /**
   * Starts performance monitoring only for whole system. After call of this method you can not start monitoring on thread level
   * till call of {@link #stopMonitoring()} is performed.
   */
  public void startMonitoring() {
    switchLock.acquireWriteLock();
    try {
      if (!statistics.isEmpty() && !enabled)
        throw new IllegalStateException("Monitoring is already started on thread level and can not be started on system level");

      deadThreadsStatistic = null;
      postMeasurementStatistic = null;

      enabled = true;
    } finally {
      switchLock.releaseWriteLock();
    }
  }

  /**
   * Stops monitoring of performance statistic for whole system.
   */
  public void stopMonitoring() {
    switchLock.acquireWriteLock();
    try {
      enabled = false;

      final PerformanceCountersHolder countersHolder = ComponentType.GENERAL.newCountersHolder();
      final Map<String, PerformanceCountersHolder> componentCountersHolder = new HashMap<>();

      WritCacheCountersHolder writCacheCountersHolder = deadThreadsStatistic.writCacheCountersHolder;
      StorageCountersHolder storageCountersHolder = deadThreadsStatistic.storageCountersHolder;
      WALCountersHolder walCountersHolder = deadThreadsStatistic.walCountersHolder;

      deadThreadsStatistic.countersHolder.pushData(countersHolder);
      componentCountersHolder.putAll(deadThreadsStatistic.countersByComponents);

      deadThreadsStatistic = null;

      for (OSessionStoragePerformanceStatistic statistic : statistics.values()) {
        statistic.pushSystemCounters(countersHolder);
        statistic.pushComponentCounters(componentCountersHolder);
        writCacheCountersHolder = statistic.pushWriteCacheCounters(writCacheCountersHolder);
        storageCountersHolder = statistic.pushStorageCounters(storageCountersHolder);
        walCountersHolder = statistic.pushWALCounters(walCountersHolder);
      }

      statistics.clear();

      postMeasurementStatistic = new ImmutableStatistic(countersHolder, componentCountersHolder, writCacheCountersHolder,
          storageCountersHolder, walCountersHolder);
    } finally {
      switchLock.releaseWriteLock();
    }
  }

  /**
   * Registers JMX bean for current manager.
   *
   * @param storageName Name of storage of given manager
   * @param storageId   Id of storage of given manager
   * @see OStorage#getName()
   * @see OIdentifiableStorage#getId()
   */
  public void registerMBean(String storageName, int storageId) {
    if (mbeanIsRegistered.compareAndSet(false, true)) {
      try {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final ObjectName mbeanName = new ObjectName(getMBeanName(storageName, storageId));
        if (!server.isRegistered(mbeanName)) {
          server.registerMBean(new OPerformanceStatisticManagerMBean(this), mbeanName);
        } else {
          mbeanIsRegistered.set(false);
          OLogManager.instance().warn(this,
              "MBean with name %s has already registered. Probably your system was not shutdown correctly"
                  + " or you have several running applications which use OrientDB engine inside", mbeanName.getCanonicalName());
        }

      } catch (MalformedObjectNameException | InstanceAlreadyExistsException | NotCompliantMBeanException | MBeanRegistrationException e) {
        throw OException.wrapException(new OStorageException("Error during registration of profiler MBean"), e);
      }
    }
  }

  private String getMBeanName(String storageName, int storageId) {
    return MBEAN_PREFIX + ",name=" + ObjectName.quote(storageName) + ",id=" + storageId;
  }

  /**
   * Deregisters JMX bean for current manager.
   *
   * @param storageName Name of storage of given manager
   * @param storageId   Id of storage of given manager
   * @see OStorage#getName()
   * @see OIdentifiableStorage#getId()
   */
  public void unregisterMBean(String storageName, int storageId) {
    if (storageName == null) {
      OLogManager.instance().warnNoDb(this, "Can not unregister MBean for performance statistics, storage name is null");
    }
    if (mbeanIsRegistered.compareAndSet(true, false)) {
      try {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final ObjectName mbeanName = new ObjectName(getMBeanName(storageName, storageId));
        server.unregisterMBean(mbeanName);
      } catch (MalformedObjectNameException | InstanceNotFoundException | MBeanRegistrationException e) {
        throw OException.wrapException(new OStorageException("Error during unregistration of profiler MBean"), e);
      }
    }
  }

  /**
   * Registers component as such one which participates in performance monitoring.
   *
   * @param component Component which participates in performance monitoring.
   */
  public void registerComponent(String component) {
    componentNames.add(component);
  }

  /**
   * @return Set of names of components for which performance statistic is gathered.
   */
  public Set<String> getComponentNames() {
    return new HashSet<>(componentNames);
  }

  /**
   * @return Instance of performance container which is used to gathering data about storage performance statistic or
   * <code>null</code> if none of both methods {@link #startMonitoring()} or {@link #startThreadMonitoring()} are called.
   */
  public OSessionStoragePerformanceStatistic getSessionPerformanceStatistic() {
    if (!enabled && !enabledForCurrentThread.get())
      return null;

    switchLock.acquireReadLock();
    try {
      if (!enabled && !enabledForCurrentThread.get())
        return null;

      final Thread currentThread = Thread.currentThread();
      OSessionStoragePerformanceStatistic performanceStatistic = statistics.get(currentThread);

      if (performanceStatistic != null) {
        return performanceStatistic;
      }

      performanceStatistic = new OSessionStoragePerformanceStatistic(intervalBetweenSnapshots, enabled ? cleanUpInterval : -1);

      statistics.put(currentThread, performanceStatistic);

      return performanceStatistic;
    } finally {
      switchLock.releaseReadLock();
    }
  }

  /**
   * Average amount of pages which were read from cache for component with given name during single data operation. If null value is
   * passed or data for component with passed in name does not exist then <code>-1</code> will be returned.
   *
   * @param componentName Name of component data of which should be returned. Name is case sensitive.
   * @return Average amount of pages which were read from cache for component with given name during single data operation or value
   * which is less than 0, which means that value cannot be calculated.
   */
  public long getAmountOfPagesPerOperation(String componentName) {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        final PerformanceCountersHolder componentCountersHolder = ComponentType.GENERAL.newCountersHolder();
        fetchComponentCounters(componentName, componentCountersHolder);
        return componentCountersHolder.getAmountOfPagesPerOperation();
      } else {
        final ImmutableStatistic post = postMeasurementStatistic;

        if (post == null)
          return -1;

        final PerformanceCountersHolder holder = post.countersByComponents.get(componentName);
        if (holder == null)
          return -1;

        return holder.getAmountOfPagesPerOperation();
      }
    } finally {
      switchLock.releaseReadLock();
    }
  }

  /**
   * @return Percent of cache hits or value which is less than 0, which means that value cannot be calculated.
   */
  public int getCacheHits() {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        final PerformanceCountersHolder countersHolder = ComponentType.GENERAL.newCountersHolder();
        fetchSystemCounters(countersHolder);
        return countersHolder.getCacheHits();
      } else {
        final ImmutableStatistic post = postMeasurementStatistic;

        if (post == null)
          return -1;

        return post.countersHolder.getCacheHits();
      }
    } finally {
      switchLock.releaseReadLock();
    }
  }

  /**
   * Percent of cache hits for component name of which is passed as method argument. If null value is passed then value for whole
   * system will be returned. If data for component with passed in name does not exist then <code>-1</code> will be returned.
   *
   * @param componentName Name of component data of which should be returned. Name is case sensitive.
   * @return Percent of cache hits or value which is less than 0, which means that value cannot be calculated.
   */
  public int getCacheHits(String componentName) {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        final PerformanceCountersHolder countersHolder = ComponentType.GENERAL.newCountersHolder();
        fetchComponentCounters(componentName, countersHolder);
        return countersHolder.getCacheHits();
      } else {
        final ImmutableStatistic post = postMeasurementStatistic;
        if (post == null)
          return -1;

        final PerformanceCountersHolder holder = post.countersByComponents.get(componentName);
        if (holder != null)
          return holder.getCacheHits();

        return -1;
      }
    } finally {
      switchLock.releaseReadLock();
    }
  }

  /**
   * @return Average time of commit of atomic operation in nanoseconds or value which is less than 0, which means that value cannot
   * be calculated.
   */
  public long getCommitTime() {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        final PerformanceCountersHolder countersHolder = ComponentType.GENERAL.newCountersHolder();
        fetchSystemCounters(countersHolder);
        return countersHolder.getCommitTime();
      } else {
        final ImmutableStatistic post = postMeasurementStatistic;
        if (post == null)
          return -1;

        return post.countersHolder.getCommitTime();
      }
    } finally {
      switchLock.releaseReadLock();
    }
  }

  /**
   * @return Read speed of data in pages per second on cache level or value which is less than 0, which means that value cannot be
   * calculated.
   */
  public long getReadSpeedFromCacheInPages() {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        final PerformanceCountersHolder countersHolder = ComponentType.GENERAL.newCountersHolder();
        fetchSystemCounters(countersHolder);
        return countersHolder.getReadSpeedFromCacheInPages();
      } else {
        final ImmutableStatistic post = postMeasurementStatistic;
        if (post == null)
          return -1;

        return post.countersHolder.getReadSpeedFromCacheInPages();
      }
    } finally {
      switchLock.releaseReadLock();
    }
  }

  /**
   * Read speed of data in pages per second on cache level for component name of which is passed as method argument. If null value
   * is passed then value for whole system will be returned. If data for component with passed in name does not exist then
   * <code>-1</code> will be returned.
   *
   * @param componentName Name of component data of which should be returned. Name is case sensitive.
   * @return Read speed of data in pages per second on cache level or value which is less than 0, which means that value cannot be
   * calculated.
   */
  public long getReadSpeedFromCacheInPages(String componentName) {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        final PerformanceCountersHolder countersHolder = ComponentType.GENERAL.newCountersHolder();
        fetchComponentCounters(componentName, countersHolder);
        return countersHolder.getReadSpeedFromCacheInPages();
      } else {
        final ImmutableStatistic post = postMeasurementStatistic;
        if (post == null)
          return -1;

        final PerformanceCountersHolder holder = post.countersByComponents.get(componentName);
        if (holder == null)
          return -1;

        return holder.getReadSpeedFromCacheInPages();
      }

    } finally {
      switchLock.releaseReadLock();
    }
  }

  /**
   * @return Read speed of data from file system in pages per second or value which is less than 0, which means that value cannot be
   * calculated.
   */
  public long getReadSpeedFromFileInPages() {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        final PerformanceCountersHolder countersHolder = ComponentType.GENERAL.newCountersHolder();
        fetchSystemCounters(countersHolder);
        return countersHolder.getReadSpeedFromFileInPages();
      } else {
        final ImmutableStatistic post = postMeasurementStatistic;
        if (post == null)
          return -1;

        return post.countersHolder.getReadSpeedFromFileInPages();
      }
    } finally {
      switchLock.releaseReadLock();
    }
  }

  /**
   * Read speed of data from file system in pages for component name of which is passed as method argument. If null value is passed
   * then value for whole system will be returned. If data for component with passed in name does not exist then <code>-1</code>
   * will be returned.
   *
   * @param componentName Name of component data of which should be returned. Name is case sensitive.
   * @return Read speed of data from file system in pages per second or value which is less than 0, which means that value cannot be
   * calculated.
   */
  public long getReadSpeedFromFileInPages(String componentName) {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        final PerformanceCountersHolder countersHolder = ComponentType.GENERAL.newCountersHolder();
        fetchComponentCounters(componentName, countersHolder);
        return countersHolder.getReadSpeedFromFileInPages();
      } else {
        final ImmutableStatistic post = postMeasurementStatistic;
        if (post == null)
          return -1;

        final PerformanceCountersHolder holder = post.countersByComponents.get(componentName);
        if (holder == null)
          return -1;

        return holder.getReadSpeedFromFileInPages();
      }
    } finally {
      switchLock.releaseReadLock();
    }
  }

  /**
   * @return Write speed of data in pages per second on cache level or value which is less than 0, which means that value cannot be
   * calculated.
   */
  public long getWriteSpeedInCacheInPages() {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        final PerformanceCountersHolder countersHolder = ComponentType.GENERAL.newCountersHolder();
        fetchSystemCounters(countersHolder);
        return countersHolder.getWriteSpeedInCacheInPages();
      } else {
        final ImmutableStatistic post = postMeasurementStatistic;

        if (post == null)
          return -1;

        return post.countersHolder.getWriteSpeedInCacheInPages();
      }
    } finally {
      switchLock.releaseReadLock();
    }
  }

  /**
   * Write speed of data in pages per second on cache level for component name of which is passed as method argument. If null value
   * is passed then value for whole system will be returned. If data for component with passed in name does not exist then
   * <code>-1</code> will be returned.
   *
   * @param componentName Name of component data of which should be returned. Name is case sensitive.
   * @return Write speed of data in pages per second on cache level or value which is less than 0, which means that value cannot be
   * calculated.
   */
  public long getWriteSpeedInCacheInPages(String componentName) {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        final PerformanceCountersHolder countersHolder = ComponentType.GENERAL.newCountersHolder();
        fetchComponentCounters(componentName, countersHolder);
        return countersHolder.getWriteSpeedInCacheInPages();
      } else {
        final ImmutableStatistic post = postMeasurementStatistic;
        if (post == null)
          return -1;

        final PerformanceCountersHolder holder = post.countersByComponents.get(componentName);
        if (holder == null)
          return -1;

        return holder.getWriteSpeedInCacheInPages();
      }
    } finally {
      switchLock.releaseReadLock();
    }
  }

  /**
   * @return Amount of pages which are flushed during "page flush" operation of write cache or <code>-1</code> if value is
   * undefined.
   */
  public long getWriteCachePagesPerFlush() {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        WritCacheCountersHolder holder = fetchWriteCacheCounters();
        if (holder != null)
          return holder.getPagesPerFlush();

        return -1;
      } else {
        final ImmutableStatistic post = postMeasurementStatistic;
        if (post == null)
          return -1;

        final WritCacheCountersHolder holder = post.writCacheCountersHolder;
        if (holder != null)
          return holder.getPagesPerFlush();

        return -1;
      }
    } finally {
      switchLock.releaseReadLock();
    }
  }

  /**
   * @return Amount of time which is spent on each "page flush" operation of write cache or <code>-1</code> if value is undefined.
   */
  public long getWriteCacheFlushOperationsTime() {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        WritCacheCountersHolder holder = fetchWriteCacheCounters();
        if (holder != null)
          return holder.getFlushOperationsTime();

        return -1;
      } else {
        final ImmutableStatistic post = postMeasurementStatistic;
        if (post == null)
          return -1;

        final WritCacheCountersHolder holder = post.writCacheCountersHolder;
        if (holder != null)
          return holder.getFlushOperationsTime();

        return -1;
      }
    } finally {
      switchLock.releaseReadLock();
    }
  }

  /**
   * @return Amount of time which is spent on "fuzzy checkpoint" operation or <code>-1</code> if value if undefined.
   */
  public long getWriteCacheFuzzyCheckpointTime() {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        WritCacheCountersHolder holder = fetchWriteCacheCounters();
        if (holder != null)
          return holder.getFuzzyCheckpointTime();

        return -1;
      } else {
        final ImmutableStatistic post = postMeasurementStatistic;
        if (post == null)
          return -1;

        final WritCacheCountersHolder holder = post.writCacheCountersHolder;
        if (holder != null)
          return holder.getFuzzyCheckpointTime();

        return -1;
      }
    } finally {
      switchLock.releaseReadLock();
    }
  }

  /**
   * @return Time which is spent on full checkpoint operation, or <code>-1</code> if value is undefined
   */
  public long getFullCheckpointTime() {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        StorageCountersHolder holder = fetchStorageCounters();
        if (holder != null)
          return holder.getFullCheckpointTime();

        return -1;
      } else {
        final ImmutableStatistic post = postMeasurementStatistic;
        if (post == null)
          return -1;

        final StorageCountersHolder holder = post.storageCountersHolder;
        if (holder != null)
          return holder.getFullCheckpointTime();

        return -1;
      }
    } finally {
      switchLock.releaseReadLock();
    }
  }

  /**
   * @return Amount of times full checkpoint operations is called on storage, or <code>-1</code> if this value is undefined.
   */
  public long getFullCheckpointCount() {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        fullCheckpointCount = storage.getFullCheckpointCount();
        return fullCheckpointCount;
      } else {
        return fullCheckpointCount;
      }
    } finally {
      switchLock.releaseReadLock();
    }
  }

  /**
   * @return Size of read cache in bytes or <code>-1</code> if value is undefined.
   */
  public long getReadCacheSize() {
    return -1;
  }

  /**
   * @return Size of write cache in bytes or <code>-1</code> if value is undefined.
   */
  public long getWriteCacheSize() {
    switchLock.acquireReadLock();
    try {

      if (enabled) {
        final OWOWCache cache = getWowCache();
        if (cache != null) {
          writeCacheSize = cache.getWriteCacheSize();
        }

        return writeCacheSize;
      } else {
        return writeCacheSize;
      }
    } finally {
      switchLock.releaseReadLock();
    }
  }

  /**
   * @return Size of part of write cache which exclusively belongs to only this cache or <code>-1</code> if value is undefined
   */
  public long getExclusiveWriteCacheSize() {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        final OWOWCache cache = getWowCache();
        if (cache != null) {
          exclusiveWriteCacheSize = cache.getExclusiveWriteCacheSize();
        }

        return exclusiveWriteCacheSize;
      } else {
        return exclusiveWriteCacheSize;
      }
    } finally {
      switchLock.releaseReadLock();
    }
  }

  /**
   * @return Amount of times when size of write cache is not enough to keep dirty pages and flush of write cache is forced, or
   * <code>-1</code> if this value is undefined.
   */
  public long getWriteCacheOverflowCount() {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        final OWOWCache cache = getWowCache();
        if (cache != null) {
          writeCacheOverflowCount = cache.getCacheOverflowCount();
        }

        return writeCacheOverflowCount;
      } else {
        return writeCacheOverflowCount;
      }
    } finally {
      switchLock.releaseReadLock();
    }
  }

  /**
   * @return time which is spent on logging of single record or <code>-1</code> if value is undefined.
   */
  public long getWALLogRecordTime() {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        final WALCountersHolder holder = fetchWALCounters();
        if (holder != null)
          return holder.getLogTime();

        return -1;
      } else {
        final ImmutableStatistic post = postMeasurementStatistic;
        if (post == null)
          return -1;

        final WALCountersHolder holder = post.walCountersHolder;
        if (holder != null)
          return holder.getLogTime();

        return -1;
      }
    } finally {
      switchLock.releaseReadLock();
    }
  }

  /**
   * @return Time which is spent on logging of "start atomic operation" record or <code>-1</code> if value is undefined.
   */
  public long getWALStartAOLogRecordTime() {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        final WALCountersHolder holder = fetchWALCounters();
        if (holder != null)
          return holder.getStartAOTime();

        return -1;
      } else {
        final ImmutableStatistic post = postMeasurementStatistic;
        if (post == null)
          return -1;

        final WALCountersHolder holder = post.walCountersHolder;
        if (holder != null)
          return holder.getStartAOTime();

        return -1;
      }
    } finally {
      switchLock.releaseReadLock();
    }
  }

  /**
   * @return time which is spent on logging of  "stop atomic operation" record or <code>-1</code> if value is undefined.
   */
  public long getWALStopAOLogRecordTime() {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        final WALCountersHolder holder = fetchWALCounters();
        if (holder != null)
          return holder.getStopAOTime();

        return -1;
      } else {
        final ImmutableStatistic post = postMeasurementStatistic;
        if (post == null)
          return -1;

        final WALCountersHolder holder = post.walCountersHolder;
        if (holder != null)
          return holder.getStopAOTime();

        return -1;
      }
    } finally {
      switchLock.releaseReadLock();
    }
  }

  /**
   * @return time which is spent on flush of WAL cache or <code>-1</code> if value is undefined
   */
  public long getWALFlushTime() {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        final WALCountersHolder holder = fetchWALCounters();

        if (holder != null)
          return holder.getFlushTime();

        return -1;
      } else {
        final ImmutableStatistic post = postMeasurementStatistic;
        if (post == null)
          return -1;

        final WALCountersHolder holder = post.walCountersHolder;
        if (holder != null)
          return holder.getFlushTime();

        return -1;
      }
    } finally {
      switchLock.releaseReadLock();
    }
  }

  /**
   * Iterates over all live threads and accumulates write performance statics gathered form threads, also accumulates statistic from
   * dead threads which were alive when when gathering of performance measurements is started.
   *
   * @return Aggregated write cache performance statistic
   */
  private WritCacheCountersHolder fetchWriteCacheCounters() {
    //go through all threads and accumulate statistic only for live threads
    //all dead threads will be removed and statistics from them will be
    //later accumulated in #deadThreadsStatistic field, then result statistic from this field
    //will be aggregated to countersHolder

    //To decrease inter thread communication delay we fetch snapshots first
    //and only after that we aggregate data from immutable snapshots
    final Collection<ORawPair<Thread, PerformanceSnapshot>> snapshots = new ArrayList<>(statistics.size());

    final Collection<Thread> threadsToRemove = new ArrayList<>();
    for (Map.Entry<Thread, OSessionStoragePerformanceStatistic> entry : statistics.entrySet()) {
      final Thread thread = entry.getKey();

      final OSessionStoragePerformanceStatistic statistic = entry.getValue();
      snapshots.add(new ORawPair<>(thread, statistic.getSnapshot()));
    }

    WritCacheCountersHolder holder = null;
    for (ORawPair<Thread, PerformanceSnapshot> pair : snapshots) {
      final Thread thread = pair.first;

      if (thread.isAlive()) {
        final PerformanceSnapshot snapshot = pair.second;

        if (snapshot.writCacheCountersHolder != null) {
          if (holder == null)
            holder = new WritCacheCountersHolder();

          snapshot.writCacheCountersHolder.pushData(holder);
        }
      } else {
        threadsToRemove.add(thread);
      }
    }

    if (!threadsToRemove.isEmpty()) {
      updateDeadThreadsStatistic(threadsToRemove);
    }

    final ImmutableStatistic ds = deadThreadsStatistic;
    if (ds != null) {
      final WritCacheCountersHolder wch = ds.writCacheCountersHolder;
      if (wch != null) {
        if (holder == null)
          holder = new WritCacheCountersHolder();

        wch.pushData(holder);
      }
    }

    return holder;
  }

  /**
   * Iterates over all live threads and accumulates storage performance statics gathered form threads, also accumulates statistic
   * from dead threads which were alive when when gathering of performance measurements is started.
   *
   * @return Aggregated storage performance statistic
   */
  private StorageCountersHolder fetchStorageCounters() {
    //go through all threads and accumulate statistic only for live threads
    //all dead threads will be removed and statistics from them will be
    //later accumulated in #deadThreadsStatistic field, then result statistic from this field
    //will be aggregated to countersHolder

    //To decrease inter thread communication delay we fetch snapshots first
    //and only after that we aggregate data from immutable snapshots
    final Collection<ORawPair<Thread, PerformanceSnapshot>> snapshots = new ArrayList<>(statistics.size());

    final Collection<Thread> threadsToRemove = new ArrayList<>();
    for (Map.Entry<Thread, OSessionStoragePerformanceStatistic> entry : statistics.entrySet()) {
      final Thread thread = entry.getKey();

      final OSessionStoragePerformanceStatistic statistic = entry.getValue();
      snapshots.add(new ORawPair<>(thread, statistic.getSnapshot()));
    }

    StorageCountersHolder holder = null;
    for (ORawPair<Thread, PerformanceSnapshot> pair : snapshots) {
      final Thread thread = pair.first;

      if (thread.isAlive()) {
        final PerformanceSnapshot snapshot = pair.second;

        if (snapshot.storageCountersHolder != null) {
          if (holder == null)
            holder = new StorageCountersHolder();

          snapshot.storageCountersHolder.pushData(holder);
        }
      } else {
        threadsToRemove.add(thread);
      }
    }

    if (!threadsToRemove.isEmpty()) {
      updateDeadThreadsStatistic(threadsToRemove);
    }

    final ImmutableStatistic ds = deadThreadsStatistic;
    if (ds != null) {
      final StorageCountersHolder sch = ds.storageCountersHolder;
      if (sch != null) {
        if (holder == null)
          holder = new StorageCountersHolder();

        sch.pushData(holder);
      }
    }

    return holder;
  }

  /**
   * Iterates over all live threads and accumulates write ahead log performance statics gathered form threads, also accumulates
   * statistic from dead threads which were alive when when gathering of performance measurements is started.
   *
   * @return Aggregated write ahead log performance statistic
   */
  private WALCountersHolder fetchWALCounters() {
    //go through all threads and accumulate statistic only for live threads
    //all dead threads will be removed and statistics from them will be
    //later accumulated in #deadThreadsStatistic field, then result statistic from this field
    //will be aggregated to countersHolder

    //To decrease inter thread communication delay we fetch snapshots first
    //and only after that we aggregate data from immutable snapshots
    final Collection<ORawPair<Thread, PerformanceSnapshot>> snapshots = new ArrayList<>(statistics.size());

    final Collection<Thread> threadsToRemove = new ArrayList<>();
    for (Map.Entry<Thread, OSessionStoragePerformanceStatistic> entry : statistics.entrySet()) {
      final Thread thread = entry.getKey();

      final OSessionStoragePerformanceStatistic statistic = entry.getValue();
      snapshots.add(new ORawPair<>(thread, statistic.getSnapshot()));
    }

    WALCountersHolder holder = null;
    for (ORawPair<Thread, PerformanceSnapshot> pair : snapshots) {
      final Thread thread = pair.first;

      if (thread.isAlive()) {
        final PerformanceSnapshot snapshot = pair.second;

        if (snapshot.walCountersHolder != null) {
          if (holder == null)
            holder = new WALCountersHolder();

          snapshot.walCountersHolder.pushData(holder);
        }
      } else {
        threadsToRemove.add(thread);
      }
    }

    if (!threadsToRemove.isEmpty()) {
      updateDeadThreadsStatistic(threadsToRemove);
    }

    final ImmutableStatistic ds = deadThreadsStatistic;
    if (ds != null) {
      final WALCountersHolder wch = ds.walCountersHolder;
      if (wch != null) {
        if (holder == null)
          holder = new WALCountersHolder();

        wch.pushData(holder);
      }
    }

    return holder;
  }

  /**
   * Iterates over all live threads and accumulates performance statics gathered form threads on system level, also accumulates
   * statistic from dead threads which were alive when when gathering of performance measurements is started.
   *
   * @param countersHolder Holder which is used to accumulate all performance statistic data
   */
  private void fetchSystemCounters(PerformanceCountersHolder countersHolder) {
    //go through all threads and accumulate statistic only for live threads
    //all dead threads will be removed and statistics from them will be
    //later accumulated in #deadThreadsStatistic field, then result statistic from this field
    //will be aggregated to countersHolder

    //To decrease inter thread communication delay we fetch snapshots first
    //and only after that we aggregate data from immutable snapshots
    final Collection<ORawPair<Thread, PerformanceSnapshot>> snapshots = new ArrayList<>(statistics.size());

    final Collection<Thread> threadsToRemove = new ArrayList<>();
    for (Map.Entry<Thread, OSessionStoragePerformanceStatistic> entry : statistics.entrySet()) {
      final Thread thread = entry.getKey();
      final OSessionStoragePerformanceStatistic statistic = entry.getValue();
      snapshots.add(new ORawPair<>(thread, statistic.getSnapshot()));
    }

    for (ORawPair<Thread, PerformanceSnapshot> pair : snapshots) {
      final Thread thread = pair.first;

      if (thread.isAlive()) {
        final PerformanceSnapshot snapshot = pair.second;
        snapshot.performanceCountersHolder.pushData(countersHolder);
      } else {
        threadsToRemove.add(thread);
      }
    }

    if (!threadsToRemove.isEmpty()) {
      updateDeadThreadsStatistic(threadsToRemove);
    }

    final ImmutableStatistic ds = deadThreadsStatistic;
    if (ds != null) {
      final PerformanceCountersHolder dch = ds.countersHolder;
      dch.pushData(countersHolder);
    }
  }

  /**
   * Iterates over all live threads and accumulates performance statics gathered form threads for provided component, also
   * accumulates statistic from dead threads which were alive when when gathering of performance measurements is started.
   *
   * @param componentCountersHolder Holder which is used to accumulate all performance statistic data for given component
   * @param componentName           Name of component
   */
  private void fetchComponentCounters(String componentName, PerformanceCountersHolder componentCountersHolder) {
    //go through all threads and accumulate statistic only for live threads
    //all dead threads will be removed and statistics from them will be
    //later accumulated in #deadThreadsStatistic field, then result statistic from this field
    //will be aggregated to componentCountersHolder

    //To decrease inter thread communication delay we fetch snapshots first
    //and only after that we aggregate data from immutable snapshots
    final Collection<ORawPair<Thread, PerformanceSnapshot>> snapshots = new ArrayList<>(statistics.size());

    final List<Thread> threadsToRemove = new ArrayList<>();
    for (Map.Entry<Thread, OSessionStoragePerformanceStatistic> entry : statistics.entrySet()) {
      final Thread thread = entry.getKey();
      final OSessionStoragePerformanceStatistic statistic = entry.getValue();
      snapshots.add(new ORawPair<>(thread, statistic.getSnapshot()));
    }

    for (ORawPair<Thread, PerformanceSnapshot> pair : snapshots) {
      final Thread thread = pair.first;
      if (thread.isAlive()) {
        final PerformanceSnapshot snapshot = pair.second;
        final PerformanceCountersHolder holder = snapshot.countersByComponent.get(componentName);
        if (holder != null)
          holder.pushData(componentCountersHolder);
      } else {
        threadsToRemove.add(thread);
      }
    }

    if (!threadsToRemove.isEmpty()) {
      updateDeadThreadsStatistic(threadsToRemove);
    }

    final ImmutableStatistic ds = deadThreadsStatistic;
    if (ds != null) {
      final PerformanceCountersHolder dch = ds.countersByComponents.get(componentName);
      if (dch != null) {
        dch.pushData(componentCountersHolder);
      }
    }
  }

  /**
   * Removes provided dead threads from {@link #statistics} field and accumulates data from them in {@link #deadThreadsStatistic}.
   *
   * @param threadsToRemove Dead threads statistic of which should be moved to {@link #deadThreadsStatistic} field.
   */
  private void updateDeadThreadsStatistic(Collection<Thread> threadsToRemove) {
    deadThreadsUpdateLock.lock();
    try {
      //we accumulate all statistic in intermediate fields and only then put
      //results in #deadThreadsStatistic field to preserve thread safety features
      final ImmutableStatistic oldDS = deadThreadsStatistic;
      final PerformanceCountersHolder countersHolder = ComponentType.GENERAL.newCountersHolder();
      final Map<String, PerformanceCountersHolder> countersByComponents = new HashMap<>();

      WritCacheCountersHolder writeCacheCountersHolder = null;
      StorageCountersHolder storageCountersHolder = null;
      WALCountersHolder walCountersHolder = null;

      //fetch data from old statistic first
      if (oldDS != null) {
        oldDS.countersHolder.pushData(countersHolder);

        for (Map.Entry<String, PerformanceCountersHolder> oldEntry : oldDS.countersByComponents.entrySet()) {
          final PerformanceCountersHolder holder = oldEntry.getValue().newInstance();
          oldEntry.getValue().pushData(holder);

          countersByComponents.put(oldEntry.getKey(), holder);
        }

        if (oldDS.writCacheCountersHolder != null) {
          writeCacheCountersHolder = new WritCacheCountersHolder();
          oldDS.writCacheCountersHolder.pushData(writeCacheCountersHolder);
        }

        if (oldDS.storageCountersHolder != null) {
          storageCountersHolder = new StorageCountersHolder();
          oldDS.storageCountersHolder.pushData(storageCountersHolder);
        }

        if (oldDS.walCountersHolder != null) {
          walCountersHolder = new WALCountersHolder();
          oldDS.walCountersHolder.pushData(walCountersHolder);
        }
      }

      //remove all threads from active statistic and put all in #deadThreadsStatistic field
      for (Thread deadThread : threadsToRemove) {
        final OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = statistics.remove(deadThread);
        if (sessionStoragePerformanceStatistic != null) {
          sessionStoragePerformanceStatistic.pushSystemCounters(countersHolder);
          sessionStoragePerformanceStatistic.pushComponentCounters(countersByComponents);
          writeCacheCountersHolder = sessionStoragePerformanceStatistic.pushWriteCacheCounters(writeCacheCountersHolder);
          storageCountersHolder = sessionStoragePerformanceStatistic.pushStorageCounters(storageCountersHolder);
          walCountersHolder = sessionStoragePerformanceStatistic.pushWALCounters(walCountersHolder);
        }
      }

      deadThreadsStatistic = new ImmutableStatistic(countersHolder, countersByComponents, writeCacheCountersHolder,
          storageCountersHolder, walCountersHolder);
    } finally {
      deadThreadsUpdateLock.unlock();
    }
  }

  private final class ImmutableStatistic {
    private final PerformanceCountersHolder              countersHolder;
    private final Map<String, PerformanceCountersHolder> countersByComponents;
    private final WritCacheCountersHolder                writCacheCountersHolder;
    private final StorageCountersHolder                  storageCountersHolder;
    private final WALCountersHolder                      walCountersHolder;

    public ImmutableStatistic(PerformanceCountersHolder countersHolder, Map<String, PerformanceCountersHolder> countersByComponents,
        WritCacheCountersHolder writCacheCountersHolder, StorageCountersHolder storageCountersHolder,
        WALCountersHolder walCountersHolder) {
      this.countersHolder = countersHolder;
      this.countersByComponents = countersByComponents;
      this.writCacheCountersHolder = writCacheCountersHolder;
      this.storageCountersHolder = storageCountersHolder;
      this.walCountersHolder = walCountersHolder;
    }

  }
}
