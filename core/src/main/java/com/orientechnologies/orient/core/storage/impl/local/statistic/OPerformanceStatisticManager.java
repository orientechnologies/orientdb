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
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.OIdentifiableStorage;
import com.orientechnologies.orient.core.storage.OStorage;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.orientechnologies.orient.core.storage.impl.local.statistic.OSessionStoragePerformanceStatistic.PerformanceCountersHolder;

/**
 * Aggregator of performance statistic for whole storage.
 * Allows to gather performance statistic for single thread or for all threads in storage.
 * <p>
 * In case of gathering statistics for single thread statistic for all period is gathered and average values are provided.
 * If you gather statistic for whole system time series of measurements is provided.
 * <p>
 * You can not measure statistic for whole system and for chosen threads at the same time.
 * <p>
 * To gather statistic for single thread use following workflow:
 * <ol>
 * <li>Call {@link #startThreadMonitoring()}</li>
 * <li>Execute database commands</li>
 * <li>Call {@link #stopThreadMonitoring()}</li>
 * </ol>
 * <p>
 * Instance of {@link OSessionStoragePerformanceStatistic} returned as result of call of last method will contain all
 * performance data are gathered during storage monitoring.
 * <p>
 * To gather statistic for whole system use following workflow:
 * <ol>
 * <li>Call {@link #startMonitoring()}</li>
 * <li>During monitoring of storage use getXXX methods to get information about performance numbers</li>
 * <li>At the end of monitoring call {@link #startMonitoring()}</li>
 * </ol>
 * <p>
 * You may access performance data both after you stopped gathering statistic and during gathering of statistic.
 * You may manipulate by manager directly from Java or from JMX from bean with name which consist of prefix {@link #MBEAN_PREFIX}
 * and storage name.
 */
public class OPerformanceStatisticManager implements OPerformanceStatisticManagerMXBean {
  /**
   * Prefix of name of JMX bean.
   */
  public static final String MBEAN_PREFIX = "com.orientechnologies.orient.core.storage.impl.local.statistic:type=OPerformanceStatisticManagerMXBean";

  /**
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
   * Lock for switching on/off statistic measurements.
   */
  private final OReadersWriterSpinLock switchLock = new OReadersWriterSpinLock();

  /**
   * Indicates whether gathering of performance data for whole system is switched on/off.
   */
  private boolean enabled = false;

  /**
   * Indicates whether gathering of performance data for single thread is switched on/off.
   */
  private final ThreadLocal<Boolean> enabledForCurrentThread = new ThreadLocal<Boolean>() {
    @Override
    protected Boolean initialValue() {
      return false;
    }
  };

  /**
   * Map which bounds performance statistic for currently running threads.
   */
  private final ConcurrentHashMap<Thread, OSessionStoragePerformanceStatistic> statistics = new ConcurrentHashMap<Thread, OSessionStoragePerformanceStatistic>();

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
   * @param intervalBetweenSnapshots Interval between time series for each thread statistic.
   * @see OSessionStoragePerformanceStatistic
   */
  public OPerformanceStatisticManager(long intervalBetweenSnapshots) {
    this.intervalBetweenSnapshots = intervalBetweenSnapshots;
  }

  /**
   * Starts performance monitoring only for single thread.
   * After call of this method you can not start system wide monitoring till call of {@link #stopThreadMonitoring()} is performed.
   */
  public void startThreadMonitoring() {
    switchLock.acquireWriteLock();
    try {
      if (enabled)
        throw new IllegalStateException("Monitoring is already started on system level and can not be started on thread level");

      enabledForCurrentThread.set(true);
      statistics.put(Thread.currentThread(), new OSessionStoragePerformanceStatistic(intervalBetweenSnapshots, false));
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
   * Starts performance monitoring only for whole system.
   * <p>
   * After call of this method you can not start monitoring on thread level till call of {@link #stopMonitoring()} is performed.
   */
  @Override
  public void startMonitoring() {
    switchLock.acquireWriteLock();
    try {
      if (!statistics.isEmpty() && !enabled)
        throw new IllegalStateException("Monitoring is already started on thread level and can not be started on system level");

      enabled = true;

      deadThreadsStatistic = null;
      postMeasurementStatistic = null;
    } finally {
      switchLock.releaseWriteLock();
    }
  }

  /**
   * Stops monitoring of performance statistic for whole system.
   */
  @Override
  public void stopMonitoring() {
    switchLock.acquireWriteLock();
    try {
      enabled = false;

      final PerformanceCountersHolder countersHolder = new PerformanceCountersHolder();
      final Map<String, PerformanceCountersHolder> componentCountersHolder = new HashMap<String, PerformanceCountersHolder>();

      deadThreadsStatistic.countersHolder.pushData(countersHolder);
      componentCountersHolder.putAll(deadThreadsStatistic.countersByComponents);

      deadThreadsStatistic = null;

      for (OSessionStoragePerformanceStatistic statistic : statistics.values()) {
        statistic.pushSystemCounters(countersHolder);
        statistic.pushComponentCounters(componentCountersHolder);
      }

      statistics.clear();

      postMeasurementStatistic = new ImmutableStatistic(countersHolder, componentCountersHolder);
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
          server.registerMBean(this, mbeanName);
        } else {
          mbeanIsRegistered.set(false);
          OLogManager.instance().warn(this,
              "MBean with name %s has already registered. Probably your system was not shutdown correctly"
                  + " or you have several running applications which use OrientDB engine inside", mbeanName.getCanonicalName());
        }

      } catch (MalformedObjectNameException e) {
        throw OException.wrapException(new OStorageException("Error during registration of profiler MBean"), e);
      } catch (InstanceAlreadyExistsException e) {
        throw OException.wrapException(new OStorageException("Error during registration of profiler MBean"), e);
      } catch (MBeanRegistrationException e) {
        throw OException.wrapException(new OStorageException("Error during registration of profiler MBean"), e);
      } catch (NotCompliantMBeanException e) {
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
    if (mbeanIsRegistered.compareAndSet(true, false)) {
      try {
        final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        final ObjectName mbeanName = new ObjectName(getMBeanName(storageName, storageId));
        server.unregisterMBean(mbeanName);
      } catch (MalformedObjectNameException e) {
        throw OException.wrapException(new OStorageException("Error during unregistration of profiler MBean"), e);
      } catch (InstanceNotFoundException e) {
        throw OException.wrapException(new OStorageException("Error during unregistration of profiler MBean"), e);
      } catch (MBeanRegistrationException e) {
        throw OException.wrapException(new OStorageException("Error during unregistration of profiler MBean"), e);
      }
    }
  }

  /**
   * @return Set of names of components for which performance statistic is gathered.
   */
  @Override
  public Set<String> getComponentNames() {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        final Set<String> result = new HashSet<String>();

        final ImmutableStatistic ds = deadThreadsStatistic;
        if (ds != null) {
          result.addAll(deadThreadsStatistic.countersByComponents.keySet());
        }

        for (final OSessionStoragePerformanceStatistic statistic : statistics.values()) {
          final Map<String, PerformanceCountersHolder> countersHolderMap = new ConcurrentHashMap<String, PerformanceCountersHolder>();
          statistic.pushComponentCounters(countersHolderMap);
          result.addAll(countersHolderMap.keySet());
        }

        return result;
      }

      if (postMeasurementStatistic == null)
        return Collections.emptySet();

      return Collections.unmodifiableSet(postMeasurementStatistic.countersByComponents.keySet());
    } finally {
      switchLock.releaseReadLock();
    }
  }

  /**
   * @return Instance of performance container which is used to gathering data about storage performance statistic or
   * <code>null</code> if none of both methods {@link #startMonitoring()} or {@link #startThreadMonitoring()} are called.
   */
  @Override
  public OSessionStoragePerformanceStatistic getSessionPerformanceStatistic() {
    switchLock.acquireReadLock();
    try {
      if (!enabled && !enabledForCurrentThread.get())
        return null;

      final Thread currentThread = Thread.currentThread();
      OSessionStoragePerformanceStatistic performanceStatistic = statistics.get(currentThread);

      if (performanceStatistic != null) {
        return performanceStatistic;
      }

      performanceStatistic = new OSessionStoragePerformanceStatistic(intervalBetweenSnapshots, enabled);
      statistics.put(currentThread, performanceStatistic);

      return performanceStatistic;
    } finally {
      switchLock.releaseReadLock();
    }
  }

  /**
   * Average amount of pages which were read from cache for component with given name during single data operation.
   * <p>
   * If null value is passed or data for component with passed in name does not exist then <code>-1</code> will be returned.
   *
   * @param componentName Name of component data of which should be returned. Name is case sensitive.
   * @return Average amount of pages which were read from cache for component with given name during single data operation or value
   * which is less than 0, which means that value cannot be calculated.
   */

  @Override
  public long getAmountOfPagesPerOperation(String componentName) {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        final PerformanceCountersHolder componentCountersHolder = new PerformanceCountersHolder();
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
  @Override
  public int getCacheHits() {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        final PerformanceCountersHolder countersHolder = new PerformanceCountersHolder();
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
  @Override
  public int getCacheHits(String componentName) {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        final PerformanceCountersHolder countersHolder = new PerformanceCountersHolder();
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
  @Override
  public long getCommitTimeAvg() {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        final PerformanceCountersHolder countersHolder = new PerformanceCountersHolder();
        fetchSystemCounters(countersHolder);
        return countersHolder.getCommitTimeAvg();
      } else {
        final ImmutableStatistic post = postMeasurementStatistic;
        if (post == null)
          return -1;

        return post.countersHolder.getCommitTimeAvg();
      }
    } finally {
      switchLock.releaseReadLock();
    }
  }

  /**
   * @return Read speed of data in pages per second on cache level or value which is less than 0, which means that value cannot be
   * calculated.
   */
  @Override
  public long getReadSpeedFromCacheInPages() {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        final PerformanceCountersHolder countersHolder = new PerformanceCountersHolder();
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
  @Override
  public long getReadSpeedFromCacheInPages(String componentName) {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        final PerformanceCountersHolder countersHolder = new PerformanceCountersHolder();
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
  @Override
  public long getReadSpeedFromFileInPages() {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        final PerformanceCountersHolder countersHolder = new PerformanceCountersHolder();
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
  @Override
  public long getReadSpeedFromFileInPages(String componentName) {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        final PerformanceCountersHolder countersHolder = new PerformanceCountersHolder();
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
  @Override
  public long getWriteSpeedInCacheInPages() {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        final PerformanceCountersHolder countersHolder = new PerformanceCountersHolder();
        fetchSystemCounters(countersHolder);
        return countersHolder.getWriteSpeedInCacheInPages();
      } else {
        final ImmutableStatistic post = postMeasurementStatistic;
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
  @Override
  public long getWriteSpeedInCacheInPages(String componentName) {
    switchLock.acquireReadLock();
    try {
      if (enabled) {
        final PerformanceCountersHolder countersHolder = new PerformanceCountersHolder();
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
   * Iterates over all live threads and accumulates performance statics gathered form threads on system level,
   * also accumulates statistic from dead threads which were alive when when gathering of performance measurements is started.
   *
   * @param countersHolder Holder which is used to accumulate all performance statistic data
   */
  private void fetchSystemCounters(PerformanceCountersHolder countersHolder) {
    //go through all threads and accumulate statistic only for live threads
    //all dead threads will be removed and statistics from them will be
    //later accumulated in #deadThreadsStatistic field, then result statistic from this field
    //will be aggregated to countersHolder
    final ArrayList<Thread> threadsToRemove = new ArrayList<Thread>();
    for (Map.Entry<Thread, OSessionStoragePerformanceStatistic> entry : statistics.entrySet()) {
      final Thread thread = entry.getKey();

      if (!thread.isAlive()) {
        threadsToRemove.add(thread);
      } else {
        final OSessionStoragePerformanceStatistic performanceStatistic = entry.getValue();
        performanceStatistic.pushSystemCounters(countersHolder);
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
   * Iterates over all live threads and accumulates performance statics gathered form threads for provided component,
   * also accumulates statistic from dead threads which were alive when when gathering of performance measurements is started.
   *
   * @param componentCountersHolder Holder which is used to accumulate all performance statistic data for given component
   * @param componentName           Name of component
   */
  private void fetchComponentCounters(String componentName, PerformanceCountersHolder componentCountersHolder) {
    //go through all threads and accumulate statistic only for live threads
    //all dead threads will be removed and statistics from them will be
    //later accumulated in #deadThreadsStatistic field, then result statistic from this field
    //will be aggregated to componentCountersHolder

    final ArrayList<Thread> threadsToRemove = new ArrayList<Thread>();
    for (Map.Entry<Thread, OSessionStoragePerformanceStatistic> entry : statistics.entrySet()) {
      final Thread thread = entry.getKey();

      if (!thread.isAlive()) {
        threadsToRemove.add(thread);
      } else {
        final OSessionStoragePerformanceStatistic performanceStatistic = entry.getValue();
        performanceStatistic.pushComponentCounters(componentName, componentCountersHolder);
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
  private void updateDeadThreadsStatistic(ArrayList<Thread> threadsToRemove) {
    deadThreadsUpdateLock.lock();
    try {
      //we accumulate all statistic in intermediate fields and only then put
      //results in #deadThreadsStatistic field to preserve thread safety features
      final ImmutableStatistic oldDS = deadThreadsStatistic;
      final PerformanceCountersHolder countersHolder = new PerformanceCountersHolder();
      final Map<String, PerformanceCountersHolder> countersByComponents = new HashMap<String, PerformanceCountersHolder>();

      //fetch data from old statistic first
      if (oldDS != null) {
        oldDS.countersHolder.pushData(countersHolder);

        for (Map.Entry<String, PerformanceCountersHolder> oldEntry : oldDS.countersByComponents.entrySet()) {
          final PerformanceCountersHolder holder = new PerformanceCountersHolder();
          oldEntry.getValue().pushData(holder);

          countersByComponents.put(oldEntry.getKey(), holder);
        }
      }

      //remove all threads from active statistic and put all in #deadThreadsStatistic field
      for (Thread deadThread : threadsToRemove) {
        final OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = statistics.remove(deadThread);
        if (sessionStoragePerformanceStatistic != null) {
          sessionStoragePerformanceStatistic.pushSystemCounters(countersHolder);
          sessionStoragePerformanceStatistic.pushComponentCounters(countersByComponents);
        }
      }

      deadThreadsStatistic = new ImmutableStatistic(countersHolder, countersByComponents);
    } finally {
      deadThreadsUpdateLock.unlock();
    }
  }

  private final class ImmutableStatistic {
    final PerformanceCountersHolder              countersHolder;
    final Map<String, PerformanceCountersHolder> countersByComponents;

    public ImmutableStatistic(PerformanceCountersHolder countersHolder,
        Map<String, PerformanceCountersHolder> countersByComponents) {
      this.countersHolder = countersHolder;
      this.countersByComponents = countersByComponents;
    }

  }
}
