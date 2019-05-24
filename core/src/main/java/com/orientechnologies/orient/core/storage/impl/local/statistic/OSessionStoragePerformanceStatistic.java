/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.storage.impl.local.statistic;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/**
 * Container for performance statistic gathered after call of
 * {@link OAbstractPaginatedStorage#startGatheringPerformanceStatisticForCurrentThread()}.
 * <p>
 * Statistic is gathered on component and system level. Each
 * {@link com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent} provides separate data for this
 * tool which allows to detect performance problems on component level.
 * <p>
 * To stop gathering of performance statistic call
 * {@link OAbstractPaginatedStorage#completeGatheringPerformanceStatisticForCurrentThread()}.
 * <p>
 * List of gathered performance characteristics can be deduced from getXXX methods. There are 2 kind of methods , one kind do not
 * accept any parameters, they return performance data on system level and, other kind accept component name, they return
 * performance data gathered on separate component or on system level if null is passed as method name. If data from component with
 * passed in name is absent then -1 is returned.
 * <p>
 * Some data are gathered only on system level for example write ahead log performance data or commit time {@link #getCommitTime()}.
 * <p>
 * This container may be used both for gathering information in single thread and in multithreaded environment. Container itself is
 * not thread safe, but it supports data snapshots.
 * <p>
 * Every time when new data are gathered by container it checks whether minimum interval between two snapshots is passed and makes
 * data snapshot if needed, data snapshot can be used by aggregator which merges statistics from all containers together. Only last
 * snapshot is hold by container.
 * <p>
 * Also container supports time series functionality by clearing of previous statics after provided time interval.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @see OPerformanceStatisticManager
 */
public class OSessionStoragePerformanceStatistic {
  /**
   * Amount of nanoseconds in second
   */
  private static final int NANOS_IN_SECOND = 1000000000;

  /**
   * Stack of time stamps which is used to init clock in startTimerXXX methods.
   */
  private final Deque<Long> timeStamps = new ArrayDeque<>();

  /**
   * Stack of active components or in another words components which currently perform actions inside current thread.
   */
  private final Deque<Component> componentsStack = new ArrayDeque<>();

  /**
   * Container for performance counters of system performance as whole.
   * <p>
   * Counters are put in separate class because each component also has given performance counters, so definition of counters on
   * component and system level is reused.
   */
  private final PerformanceCountersHolder performanceCountersHolder;

  /**
   * Object which is used to get current PC nano time.
   */
  private final NanoTimer nanoTimer;

  /**
   * Minimum interval in nanoseconds between two snapshots
   */
  private final long intervalBetweenSnapshots;

  /**
   * Interval between two time series in nanoseconds. When this interval is passed, new attempt to make data snapshot will clear
   * performance statistic. <code>-1</code> means that performance data will not be cleared.
   */
  private final long cleanUpInterval;

  /**
   * Time stamp in nanoseconds when last snapshot is taken by container.
   */
  private long lastSnapshotTimestamp = -1;

  /**
   * Time stamp in nanoseconds when data was cleaned up last time.
   */
  private long lastCleanUpTimeStamp = -1;

  /**
   * Snapshot of all data measured during session.
   */
  private volatile PerformanceSnapshot snapshot;

  /**
   * Map containing performance counters specific for concrete software component.
   */
  private final Map<String, PerformanceCountersHolder> countersByComponent = new HashMap<>();

  /**
   * Performance statistic gathered from write cache. It is lazy initialized to decrease memory consumption.
   */
  private WritCacheCountersHolder writCacheCountersHolder;

  /**
   * Storage performance characteristics. It is lazy initialized to decrease memory consumption.
   */
  private StorageCountersHolder storageCountersHolder;

  /**
   * Write ahead log performance characteristics. It is lazy initialized to decrease memory consumption.
   */
  private WALCountersHolder walCountersHolder;

  /**
   * @param intervalBetweenSnapshots Minimum interval between two snapshots taken by container.
   * @param cleanUpInterval          Minimum interval between two time series, in other words container clears all statistic during
   *                                 next try of making
   *                                 snapshot after this interval is over. <code>-1</code> means that data will not be cleared.
   */
  public OSessionStoragePerformanceStatistic(long intervalBetweenSnapshots, long cleanUpInterval) {
    this(intervalBetweenSnapshots, System::nanoTime, cleanUpInterval);
  }

  /**
   * @param nanoTimer                Service to get current value of PC nano time.
   * @param intervalBetweenSnapshots Minimum interval between two snapshots taken by container.
   * @param cleanUpInterval          Minimum interval between two time series, in other words container clears all statistic during
   *                                 next try of making
   *                                 snapshot after this interval is over. <code>-1</code> means that data will not be cleared.
   */
  public OSessionStoragePerformanceStatistic(long intervalBetweenSnapshots, NanoTimer nanoTimer, long cleanUpInterval) {
    this.nanoTimer = nanoTimer;
    this.intervalBetweenSnapshots = intervalBetweenSnapshots;
    this.performanceCountersHolder = ComponentType.GENERAL.newCountersHolder();
    this.cleanUpInterval = cleanUpInterval;
    this.lastCleanUpTimeStamp = nanoTimer.getNano();
  }

  /**
   * Called inside of {@link com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent} to notify that
   * component started to perform operation on data. After that all performance characteristic started to be gathered for this
   * component till method {@link #completeComponentOperation()} will be called.
   * <p>
   * Components can be stacked, so if components <code>c1</code> and then <code>c2</code> call this method than performance data for
   * both components at once started to be gathered.
   *
   * @param componentName Name of component which started to perform operation on data. Name is case sensitive.
   */
  public void startComponentOperation(String componentName, ComponentType type) {
    final Component currentComponent = componentsStack.peek();

    if (currentComponent != null && componentName.equals(currentComponent.name)) {
      currentComponent.operationCount++;
      return;
    }

    componentsStack.push(new Component(componentName, type));
  }

  /**
   * Indicates that the most earliest component in stack of components has completed it's operation, so performance data for this
   * component is stopped to be gathered.
   *
   * @see #startComponentOperation(String, ComponentType)
   */
  public void completeComponentOperation() {
    final Component currentComponent = componentsStack.peek();
    if (currentComponent == null)
      return;

    currentComponent.operationCount--;

    if (currentComponent.operationCount == 0) {
      final String componentName = currentComponent.name;

      PerformanceCountersHolder cHolder = countersByComponent
          .computeIfAbsent(componentName, k -> currentComponent.type.newCountersHolder());

      cHolder.operationsCount++;

      componentsStack.pop();

      makeSnapshotIfNeeded(-1);
    }
  }

  /**
   * @return Read speed of data in pages per second on cache level or value which is less than 0, which means that value cannot be
   * calculated.
   */
  public long getReadSpeedFromCacheInPages() {
    return performanceCountersHolder.getReadSpeedFromCacheInPages();
  }

  /**
   * Read speed of data in pages per second on cache level for component name of which is passed as method argument. If null value
   * is passed then value for whole system will be returned. If data for component with passed in name does not exist then
   * <code>-1</code> will be returned.
   *
   * @param componentName Name of component data of which should be returned. Name is case sensitive.
   *
   * @return Read speed of data in pages per second on cache level or value which is less than 0, which means that value cannot be
   * calculated.
   */
  public long getReadSpeedFromCacheInPages(String componentName) {
    if (componentName == null)
      return performanceCountersHolder.getReadSpeedFromCacheInPages();

    final PerformanceCountersHolder cHolder = countersByComponent.get(componentName);
    if (cHolder != null)
      return cHolder.getReadSpeedFromCacheInPages();

    return -1;
  }

  /**
   * @return Read speed of data from file system in pages per second or value which is less than 0, which means that value cannot be
   * calculated.
   */
  public long getReadSpeedFromFileInPages() {
    return performanceCountersHolder.getReadSpeedFromFileInPages();
  }

  /**
   * Read speed of data from file system in pages for component name of which is passed as method argument. If null value is passed
   * then value for whole system will be returned. If data for component with passed in name does not exist then <code>-1</code>
   * will be returned.
   *
   * @param componentName Name of component data of which should be returned. Name is case sensitive.
   *
   * @return Read speed of data from file system in pages per second or value which is less than 0, which means that value cannot be
   * calculated.
   */
  public long getReadSpeedFromFileInPages(String componentName) {
    if (componentName == null)
      return performanceCountersHolder.getReadSpeedFromFileInPages();

    final PerformanceCountersHolder cHolder = countersByComponent.get(componentName);
    if (cHolder != null)
      return cHolder.getReadSpeedFromFileInPages();

    return -1;
  }

  /**
   * @return Amount of pages read from cache in total.
   */
  public long getAmountOfPagesReadFromCache() {
    return performanceCountersHolder.getAmountOfPagesReadFromCache();
  }

  /**
   * Amount of pages read from cache for component name of which is passed as method argument. If null value is passed then value
   * for whole system will be returned. If data for component with passed in name does not exist then <code>-1</code> will be
   * returned.
   *
   * @param componentName Name of component data of which should be returned. Name is case sensitive.
   *
   * @return Amount of pages read from cache in total.
   */
  public long getAmountOfPagesReadFromCache(String componentName) {
    if (componentName == null)
      return performanceCountersHolder.getAmountOfPagesReadFromCache();

    final PerformanceCountersHolder cHolder = countersByComponent.get(componentName);
    if (cHolder != null)
      return cHolder.getAmountOfPagesReadFromCache();

    return -1;
  }

  /**
   * @return Amount of pages are read from file.
   */
  public long getAmountOfPagesReadFromFile() {
    return performanceCountersHolder.getAmountOfPagesReadFromFile();
  }

  /**
   * Amount of pages are read from file for component name of which is passed as method argument. If null value is passed then value
   * for whole system will be returned. If data for component with passed in name does not exist then <code>-1</code> will be
   * returned.
   *
   * @param componentName Name of component data of which should be returned. Name is case sensitive.
   *
   * @return Amount of pages are read from file.
   */
  public long getAmountOfPagesReadFromFile(String componentName) {
    if (componentName == null)
      return performanceCountersHolder.getAmountOfPagesReadFromFile();

    final PerformanceCountersHolder cHolder = countersByComponent.get(componentName);
    if (cHolder != null)
      return cHolder.getAmountOfPagesReadFromFile();

    return -1;
  }

  /**
   * @return Write speed of data in pages per second on cache level or value which is less than 0, which means that value cannot be
   * calculated.
   */
  public long getWriteSpeedInCacheInPages() {
    return performanceCountersHolder.getWriteSpeedInCacheInPages();
  }

  /**
   * Write speed of data in pages per second on cache level for component name of which is passed as method argument. If null value
   * is passed then value for whole system will be returned. If data for component with passed in name does not exist then
   * <code>-1</code> will be returned.
   *
   * @param componentName Name of component data of which should be returned. Name is case sensitive.
   *
   * @return Write speed of data in pages per second on cache level or value which is less than 0, which means that value cannot be
   * calculated.
   */
  public long getWriteSpeedInCacheInPages(String componentName) {
    if (componentName == null)
      return performanceCountersHolder.getWriteSpeedInCacheInPages();

    final PerformanceCountersHolder cHolder = countersByComponent.get(componentName);
    if (cHolder != null)
      return cHolder.getWriteSpeedInCacheInPages();

    return -1;
  }

  /**
   * @return Amount of pages written to cache.
   */
  public long getAmountOfPagesWrittenInCache() {
    return performanceCountersHolder.getAmountOfPagesWrittenInCache();
  }

  /**
   * Amount of pages written to cache for component name of which is passed as method argument. If null value is passed then value
   * for whole system will be returned. If data for component with passed in name does not exist then <code>-1</code> will be
   * returned.
   *
   * @param componentName Name of component data of which should be returned. Name is case sensitive.
   *
   * @return Amount of pages written to cache.
   */
  public long getAmountOfPagesWrittenInCache(String componentName) {
    if (componentName == null)
      return performanceCountersHolder.getAmountOfPagesWrittenInCache();

    final PerformanceCountersHolder cHolder = countersByComponent.get(componentName);
    if (cHolder != null)
      return cHolder.getAmountOfPagesWrittenInCache();

    return -1;
  }

  /**
   * @return Latest snapshot is taken by container
   */
  public PerformanceSnapshot getSnapshot() {
    return snapshot;
  }

  /**
   * @return Average time of commit of atomic operation in nanoseconds or value which is less than 0, which means that value cannot
   * be calculated.
   */
  public long getCommitTime() {
    return performanceCountersHolder.getCommitTime();
  }

  /**
   * @return Percent of cache hits or value which is less than 0, which means that value cannot be calculated.
   */
  public int getCacheHits() {
    return performanceCountersHolder.getCacheHits();
  }

  /**
   * Percent of cache hits for component name of which is passed as method argument. If null value is passed then value for whole
   * system will be returned. If data for component with passed in name does not exist then <code>-1</code> will be returned.
   *
   * @param componentName Name of component data of which should be returned. Name is case sensitive.
   *
   * @return Percent of cache hits or value which is less than 0, which means that value cannot be calculated.
   */
  public int getCacheHits(String componentName) {
    if (componentName == null)
      return performanceCountersHolder.getCacheHits();

    final PerformanceCountersHolder cHolder = countersByComponent.get(componentName);
    if (cHolder != null)
      return cHolder.getCacheHits();

    return -1;
  }

  /**
   * Average amount of pages which were read from cache for component with given name during single data operation.
   * <p>
   * If null value is passed or data for component with passed in name does not exist then <code>-1</code> will be returned.
   *
   * @param componentName Name of component data of which should be returned. Name is case sensitive.
   *
   * @return Average amount of pages which were read from cache for component with given name during single data operation or value
   * which is less than 0, which means that value cannot be calculated.
   */
  public long getAmountOfPagesPerOperation(String componentName) {
    if (componentName == null) {
      return -1;
    }

    final PerformanceCountersHolder cHolder = countersByComponent.get(componentName);
    if (cHolder != null)
      return cHolder.getAmountOfPagesPerOperation();

    return -1;
  }

  /**
   * Takes performance data are split by components from last snapshot and aggregates them with data passed inside method as
   * parameter. Result of aggregation of performance data is returned inside of passed in performance data.
   *
   * @param counters Performance data for each component.
   */
  public void pushComponentCounters(Map<String, PerformanceCountersHolder> counters) {
    if (snapshot == null)
      return;

    for (Map.Entry<String, PerformanceCountersHolder> entry : snapshot.countersByComponent.entrySet()) {
      final String componentName = entry.getKey();

      PerformanceCountersHolder holder = counters.computeIfAbsent(componentName, k -> entry.getValue().newInstance());

      entry.getValue().pushData(holder);
    }
  }

  /**
   * Takes write cache performance data from last snapshot and aggregates them with data passed inside method as parameter. Result
   * of aggregation of performance data is returned inside of passed in performance data and as result of this method call.
   *
   * @param holder Performance data for write cache may be <code>null</code>
   *
   * @return Result of aggregation of performance data
   */
  public WritCacheCountersHolder pushWriteCacheCounters(WritCacheCountersHolder holder) {
    if (snapshot == null)
      return holder;

    if (snapshot.writCacheCountersHolder == null)
      return holder;

    if (holder == null)
      holder = new WritCacheCountersHolder();

    snapshot.writCacheCountersHolder.pushData(holder);

    return holder;
  }

  /**
   * Takes storage performance data from last snapshot and aggregates them with data passed inside method as parameter. Result of
   * aggregation of performance data is returned inside of passed in performance data and as result of this method call.
   *
   * @param holder Performance data for storage may be <code>null</code>
   *
   * @return Result of aggregation of performance data
   */
  public StorageCountersHolder pushStorageCounters(StorageCountersHolder holder) {
    if (snapshot == null)
      return holder;

    if (snapshot.storageCountersHolder == null)
      return holder;

    if (holder == null)
      holder = new StorageCountersHolder();

    snapshot.storageCountersHolder.pushData(holder);

    return holder;
  }

  /**
   * Takes write ahead log data from last snapshot and aggregates them with data passed inside method as parameter. Result of
   * aggregation of performance data is returned inside of passed in performance data and as result of this method call.
   *
   * @param holder Performance data for write ahead log may be <code>null</code>
   *
   * @return Result of aggregation of performance data
   */
  public WALCountersHolder pushWALCounters(WALCountersHolder holder) {
    if (snapshot == null)
      return holder;

    if (snapshot.walCountersHolder == null)
      return holder;

    if (holder == null)
      holder = new WALCountersHolder();

    snapshot.walCountersHolder.pushData(holder);

    return holder;
  }

  /**
   * Takes performance data for whole system from last snapshot and aggregates them with data passed inside method as parameter.
   * Result of aggregation of performance data is returned inside of passed in performance data.
   *
   * @param holder Performance data for whole system.
   */
  public void pushSystemCounters(PerformanceCountersHolder holder) {
    if (snapshot == null)
      return;

    snapshot.performanceCountersHolder.pushData(holder);
  }

  /**
   * Takes performance data for component from last snapshot and aggregates them with data passed inside method as parameter. Result
   * of aggregation of performance data is returned inside of passed in performance data.
   *
   * @param name   Name of component for which performance data are gathered.
   * @param holder Performance data for given component.
   */

  public void pushComponentCounters(String name, PerformanceCountersHolder holder) {
    if (snapshot == null)
      return;

    final PerformanceCountersHolder countersHolder = snapshot.countersByComponent.get(name);

    if (countersHolder != null) {
      countersHolder.pushData(holder);
    }
  }

  /**
   * Converts properties of given class into values of fields of returned document. Names of fields equal to names of properties.
   * <p>
   * All data related to separate components are stored in field <code>dataByComponent</code> map which has type
   * {@link OType#EMBEDDEDMAP} where key of map entry is name of component, and value is document which contains the same fields as
   * high level document but with values for single component not whole system.
   * <p>
   * Write ahead log performance data are stored inside of <code>walData</code> field.
   *
   * @return Performance characteristics of storage gathered after call of
   * {@link OAbstractPaginatedStorage#startGatheringPerformanceStatisticForCurrentThread()}
   */
  public ODocument toDocument() {
    final ODocument document = performanceCountersHolder.toDocument();

    document.field("commitTime", getCommitTime(), OType.LONG);

    final Map<String, ODocument> countersMap = new HashMap<>();
    for (Map.Entry<String, PerformanceCountersHolder> entry : countersByComponent.entrySet()) {
      countersMap.put(entry.getKey(), entry.getValue().toDocument());
    }

    document.field("dataByComponent", countersMap, OType.EMBEDDEDMAP);

    if (walCountersHolder != null) {
      final ODocument wal = walCountersHolder.toDocument();
      document.field("walData", wal, OType.EMBEDDED);
    }

    return document;
  }

  /**
   * Increments counter of page accesses from cache.
   * <p>
   * If you wish to gather statistic for current durable component please call
   * {@link #startComponentOperation(String, ComponentType)} method before the call and {@link #completeComponentOperation()} after
   * the call.
   */
  public void incrementPageAccessOnCacheLevel(boolean cacheHit) {
    performanceCountersHolder.cacheAccessCount++;
    if (cacheHit)
      performanceCountersHolder.cacheHit++;

    for (Component component : componentsStack) {
      final String componentName = component.name;

      PerformanceCountersHolder cHolder = countersByComponent
          .computeIfAbsent(componentName, k -> component.type.newCountersHolder());

      cHolder.cacheAccessCount++;
      if (cacheHit)
        cHolder.cacheHit++;
    }

    makeSnapshotIfNeeded(-1);
  }

  /**
   * Starts timer which counts how much time was spent on read of page from file system.
   */
  public void startPageReadFromFileTimer() {
    pushTimer();
  }

  /**
   * Stops and records results of timer which counts how much time was spent on read of page from file system.
   * <p>
   * If you wish to gather statistic for current durable component please call
   * {@link #startComponentOperation(String, ComponentType)} method before the call and {@link #completeComponentOperation()} after
   * the call.
   *
   * @param readPages Amount of pages which were read by single call to file system.
   */
  public void stopPageReadFromFileTimer(int readPages) {
    final long endTs = nanoTimer.getNano();
    final long timeDiff = (endTs - timeStamps.pop());

    performanceCountersHolder.pageReadFromFileTime += timeDiff;
    performanceCountersHolder.pageReadFromFileCount += readPages;

    for (Component component : componentsStack) {
      final String componentName = component.name;

      PerformanceCountersHolder cHolder = countersByComponent
          .computeIfAbsent(componentName, k -> component.type.newCountersHolder());
      cHolder.pageReadFromFileTime += timeDiff;
      cHolder.pageReadFromFileCount += readPages;
    }

    final Component currentComponent = componentsStack.peek();
    if (currentComponent != null) {
      PerformanceCountersHolder currentHolder = countersByComponent.get(currentComponent.name);

      if (currentHolder.currentOperation != null) {
        currentHolder.currentOperation.incrementOperationsCounter(0, readPages);
      }
    }

    makeSnapshotIfNeeded(endTs);
  }

  /**
   * Starts timer which counts how much time was spent on operation of flush pages in write cache.
   */
  public void startWriteCacheFlushTimer() {
    pushTimer();
  }

  /**
   * Stops and records results of timer which counts how much time was spent on operation of flush pages in write cache.
   *
   * @param pagesFlushed Amount of pages were flushed during this operation.
   */
  public void stopWriteCacheFlushTimer(int pagesFlushed) {
    // lazy initialization to prevent memory consumption
    if (writCacheCountersHolder == null)
      writCacheCountersHolder = new WritCacheCountersHolder();

    final long endTs = nanoTimer.getNano();
    final long timeDiff = (endTs - timeStamps.pop());

    writCacheCountersHolder.flushOperationsCount++;

    writCacheCountersHolder.amountOfPagesFlushed += pagesFlushed;
    writCacheCountersHolder.flushOperationsTime += timeDiff;

    makeSnapshotIfNeeded(endTs);
  }

  /**
   * Starts timer which counts how much time was spent on fuzzy checkpoint operation.
   */
  public void startFuzzyCheckpointTimer() {
    pushTimer();
  }

  /**
   * Stops and records results of timer which counts how much time was spent on fuzzy checkpoint operation.
   */
  public void stopFuzzyCheckpointTimer() {
    if (writCacheCountersHolder == null)
      writCacheCountersHolder = new WritCacheCountersHolder();

    final long endTs = nanoTimer.getNano();
    final long timeDiff = (endTs - timeStamps.pop());

    writCacheCountersHolder.fuzzyCheckpointCount++;
    writCacheCountersHolder.fuzzyCheckpointTime += timeDiff;

    makeSnapshotIfNeeded(endTs);
  }

  /**
   * Starts timer which counts how much time was spent on read of page from disk cache.
   */
  public void startPageReadFromCacheTimer() {
    pushTimer();
  }

  /**
   * General method which is used ba all stopXXXTimer methods to delegate their functionality.
   */
  private void pushTimer() {
    timeStamps.push(nanoTimer.getNano());
  }

  /**
   * Stops and records results of timer which counts how much time was spent on read of page from disk cache.
   * <p>
   * If you wish to gather statistic for current durable component please call
   * {@link #startComponentOperation(String, ComponentType)} method before the call and {@link #completeComponentOperation()} after
   * the call.
   */
  public void stopPageReadFromCacheTimer() {
    final long endTs = nanoTimer.getNano();
    final long timeDiff = (endTs - timeStamps.pop());

    performanceCountersHolder.pageReadFromCacheTime += timeDiff;
    performanceCountersHolder.pageReadFromCacheCount++;

    for (Component component : componentsStack) {
      final String componentName = component.name;
      PerformanceCountersHolder cHolder = countersByComponent
          .computeIfAbsent(componentName, k -> component.type.newCountersHolder());

      cHolder.pageReadFromCacheTime += timeDiff;
      cHolder.pageReadFromCacheCount++;
    }

    final Component currentComponent = componentsStack.peek();
    if (currentComponent != null) {
      PerformanceCountersHolder currentHolder = countersByComponent.get(currentComponent.name);

      if (currentHolder.currentOperation != null) {
        currentHolder.currentOperation.incrementOperationsCounter(1, 0);
      }
    }

    makeSnapshotIfNeeded(endTs);
  }

  /**
   * Starts timer which records how much time is spent on full checkpoint
   */
  public void startFullCheckpointTimer() {
    pushTimer();
  }

  /**
   * Stops and records results of timer which counts how much time was spent on full checkpoint operation.
   */
  public void stopFullCheckpointTimer() {
    final long endTs = nanoTimer.getNano();
    final long timeDiff = (endTs - timeStamps.pop());

    if (storageCountersHolder == null)
      storageCountersHolder = new StorageCountersHolder();

    storageCountersHolder.fullCheckpointOperationsCount++;
    storageCountersHolder.fullCheckpointOperationsTime += timeDiff;

    makeSnapshotIfNeeded(endTs);
  }

  /**
   * Starts timer which counts how much time was spent on write of page to disk cache.
   */
  public void startPageWriteInCacheTimer() {
    pushTimer();
  }

  /**
   * Stops and records results of timer which counts how much time was spent to write page to disk cache.
   * <p>
   * If you wish to gather statistic for current durable component please call
   * {@link #startComponentOperation(String, ComponentType)} method before the call and {@link #completeComponentOperation()} after
   * the call.
   */
  public void stopPageWriteInCacheTimer() {
    final long endTs = nanoTimer.getNano();
    final long timeDiff = (endTs - timeStamps.pop());

    performanceCountersHolder.pageWriteToCacheTime += timeDiff;
    performanceCountersHolder.pageWriteToCacheCount++;

    for (Component component : componentsStack) {
      final String componentName = component.name;
      PerformanceCountersHolder cHolder = countersByComponent
          .computeIfAbsent(componentName, k -> component.type.newCountersHolder());

      cHolder.pageWriteToCacheTime += timeDiff;
      cHolder.pageWriteToCacheCount++;
    }

    makeSnapshotIfNeeded(endTs);
  }

  public void startRecordCreationTimer() {
    pushTimer();
  }

  public void stopRecordCreationTimer() {
    final Component component = componentsStack.peek();

    checkComponentType(component, ComponentType.CLUSTER);

    final long endTs = nanoTimer.getNano();
    final long timeDiff = (endTs - timeStamps.pop());

    OClusterCountersHolder cHolder = (OClusterCountersHolder) countersByComponent.get(component.name);
    if (cHolder == null) {
      cHolder = (OClusterCountersHolder) ComponentType.CLUSTER.newCountersHolder();
      countersByComponent.put(component.name, cHolder);
    }

    cHolder.createdRecords++;
    cHolder.timeRecordCreation += timeDiff;

    makeSnapshotIfNeeded(endTs);
  }

  public void startRecordDeletionTimer() {
    pushTimer();
  }

  public void stopRecordDeletionTimer() {
    final Component component = componentsStack.peek();

    checkComponentType(component, ComponentType.CLUSTER);

    final long endTs = nanoTimer.getNano();
    final long timeDiff = (endTs - timeStamps.pop());

    OClusterCountersHolder cHolder = (OClusterCountersHolder) countersByComponent.get(component.name);
    if (cHolder == null) {
      cHolder = (OClusterCountersHolder) ComponentType.CLUSTER.newCountersHolder();
      countersByComponent.put(component.name, cHolder);
    }

    cHolder.deletedRecords++;
    cHolder.timeRecordDeletion += timeDiff;

    makeSnapshotIfNeeded(endTs);
  }

  public void startRecordUpdateTimer() {
    pushTimer();
  }

  public void stopRecordUpdateTimer() {
    final Component component = componentsStack.peek();

    checkComponentType(component, ComponentType.CLUSTER);

    final long endTs = nanoTimer.getNano();
    final long timeDiff = (endTs - timeStamps.pop());

    OClusterCountersHolder cHolder = (OClusterCountersHolder) countersByComponent.get(component.name);
    if (cHolder == null) {
      cHolder = (OClusterCountersHolder) ComponentType.CLUSTER.newCountersHolder();
      countersByComponent.put(component.name, cHolder);
    }

    cHolder.updatedRecords++;
    cHolder.timeRecordUpdate += timeDiff;

    makeSnapshotIfNeeded(endTs);
  }

  public void startRecordReadTimer() {
    pushTimer();
  }

  public void stopRecordReadTimer() {
    final Component component = componentsStack.peek();

    checkComponentType(component, ComponentType.CLUSTER);

    final long endTs = nanoTimer.getNano();
    final long timeDiff = (endTs - timeStamps.pop());

    OClusterCountersHolder cHolder = (OClusterCountersHolder) countersByComponent.get(component.name);
    if (cHolder == null) {
      cHolder = (OClusterCountersHolder) ComponentType.CLUSTER.newCountersHolder();
      countersByComponent.put(component.name, cHolder);
    }

    cHolder.readRecords++;
    cHolder.timeRecordRead += timeDiff;

    makeSnapshotIfNeeded(endTs);
  }

  public void startIndexEntryUpdateTimer() {
    pushTimer();
  }

  public void stopIndexEntryUpdateTimer() {
    final Component component = componentsStack.peek();

    checkComponentType(component, ComponentType.INDEX);

    final long endTs = nanoTimer.getNano();
    final long timeDiff = (endTs - timeStamps.pop());

    IndexCountersHolder cHolder = (IndexCountersHolder) countersByComponent.get(component.name);
    if (cHolder == null) {
      cHolder = (IndexCountersHolder) ComponentType.INDEX.newCountersHolder();
      countersByComponent.put(component.name, cHolder);
    }

    cHolder.updatedEntries++;
    cHolder.timeUpdateEntry += timeDiff;

    makeSnapshotIfNeeded(endTs);
  }

  public void startIndexEntryDeletionTimer() {
    pushTimer();
  }

  public void stopIndexEntryDeletionTimer() {
    final Component component = componentsStack.peek();

    checkComponentType(component, ComponentType.INDEX);

    final long endTs = nanoTimer.getNano();
    final long timeDiff = (endTs - timeStamps.pop());

    IndexCountersHolder cHolder = (IndexCountersHolder) countersByComponent.get(component.name);
    if (cHolder == null) {
      cHolder = (IndexCountersHolder) ComponentType.INDEX.newCountersHolder();
      countersByComponent.put(component.name, cHolder);
    }

    cHolder.deletedEntries++;
    cHolder.timeDeleteEntry += timeDiff;

    makeSnapshotIfNeeded(endTs);
  }

  public void startIndexEntryReadTimer() {
    pushTimer();
  }

  public void stopIndexEntryReadTimer() {
    final Component component = componentsStack.peek();

    checkComponentType(component, ComponentType.INDEX);

    final long endTs = nanoTimer.getNano();
    final long timeDiff = (endTs - timeStamps.pop());

    IndexCountersHolder cHolder = (IndexCountersHolder) countersByComponent.get(component.name);
    if (cHolder == null) {
      cHolder = (IndexCountersHolder) ComponentType.INDEX.newCountersHolder();
      countersByComponent.put(component.name, cHolder);
    }

    cHolder.readEntries++;
    cHolder.timeReadEntry += timeDiff;

    makeSnapshotIfNeeded(endTs);
  }

  public void startRidBagEntryReadTimer() {
    pushTimer();
  }

  public void stopRidBagEntryReadTimer(int entriesRead) {
    final Component component = componentsStack.peek();

    checkComponentType(component, ComponentType.RIDBAG);

    final long endTs = nanoTimer.getNano();
    final long timeDiff = (endTs - timeStamps.pop());

    RidbagCountersHolder cHolder = (RidbagCountersHolder) countersByComponent.get(component.name);
    if (cHolder == null) {
      cHolder = (RidbagCountersHolder) ComponentType.INDEX.newCountersHolder();
      countersByComponent.put(component.name, cHolder);
    }

    cHolder.readEntries += entriesRead;
    cHolder.timeReadEntry += timeDiff;

    makeSnapshotIfNeeded(endTs);
  }

  public void startRidBagEntryUpdateTimer() {
    pushTimer();
  }

  public void stopRidBagEntryUpdateTimer() {
    final Component component = componentsStack.peek();

    checkComponentType(component, ComponentType.RIDBAG);

    final long endTs = nanoTimer.getNano();
    final long timeDiff = (endTs - timeStamps.pop());

    RidbagCountersHolder cHolder = (RidbagCountersHolder) countersByComponent.get(component.name);
    if (cHolder == null) {
      cHolder = (RidbagCountersHolder) ComponentType.INDEX.newCountersHolder();
      countersByComponent.put(component.name, cHolder);
    }

    cHolder.updatedEntries++;
    cHolder.timeUpdateEntry += timeDiff;

    makeSnapshotIfNeeded(endTs);
  }

  public void startRidBagEntryDeletionTimer() {
    pushTimer();
  }

  public void stopRidBagEntryDeletionTimer() {
    final Component component = componentsStack.peek();

    checkComponentType(component, ComponentType.RIDBAG);

    final long endTs = nanoTimer.getNano();
    final long timeDiff = (endTs - timeStamps.pop());

    RidbagCountersHolder cHolder = (RidbagCountersHolder) countersByComponent.get(component.name);
    if (cHolder == null) {
      cHolder = (RidbagCountersHolder) ComponentType.INDEX.newCountersHolder();
      countersByComponent.put(component.name, cHolder);
    }

    cHolder.deletedEntries++;
    cHolder.timeDeleteEntry += timeDiff;

    makeSnapshotIfNeeded(endTs);
  }

  public void startRidBagEntryLoadTimer() {
    pushTimer();
  }

  public void stopRidBagEntryLoadTimer() {
    final Component component = componentsStack.peek();

    checkComponentType(component, ComponentType.RIDBAG);

    final long endTs = nanoTimer.getNano();
    final long timeDiff = (endTs - timeStamps.pop());

    RidbagCountersHolder cHolder = (RidbagCountersHolder) countersByComponent.get(component.name);
    if (cHolder == null) {
      cHolder = (RidbagCountersHolder) ComponentType.INDEX.newCountersHolder();
      countersByComponent.put(component.name, cHolder);
    }

    cHolder.loads++;
    cHolder.loadTime += timeDiff;

    makeSnapshotIfNeeded(endTs);
  }

  /**
   * Starts timer which counts how much time was spent on atomic operation commit.
   */
  public void startCommitTimer() {
    pushTimer();
  }

  /**
   * Stops and records results of timer which counts how much time was spent on atomic operation commit.
   */
  public void stopCommitTimer() {
    final long endTs = nanoTimer.getNano();
    final long timeDiff = (endTs - timeStamps.pop());

    performanceCountersHolder.commitTime += timeDiff;
    performanceCountersHolder.commitCount++;

    makeSnapshotIfNeeded(endTs);
  }

  /**
   * Starts timer which counts how much time was spent on logging of single write ahead log record
   */
  public void startWALLogRecordTimer() {
    pushTimer();
  }

  /**
   * Stops and records results of timer which counts how much time was spent on logging of single write ahead log record.
   *
   * @param isStartRecord Indicates whether we logged "start atomic operation" record
   * @param isStopRecord  Indicates whether we logged "stop atomic operation" record
   */
  public void stopWALRecordTimer(boolean isStartRecord, boolean isStopRecord) {
    final long endTs = nanoTimer.getNano();
    final long timeDiff = (endTs - timeStamps.pop());

    if (walCountersHolder == null)
      walCountersHolder = new WALCountersHolder();

    walCountersHolder.logRecordCount++;
    walCountersHolder.logRecordTime += timeDiff;

    if (isStartRecord) {
      walCountersHolder.startRecordCount++;
      walCountersHolder.startRecordTime += timeDiff;
    } else if (isStopRecord) {
      walCountersHolder.stopRecordCount++;
      walCountersHolder.stopRecordTime += timeDiff;
    }

    makeSnapshotIfNeeded(endTs);
  }

  /**
   * Starts timer which counts how much time was spent on flushing of data from write ahead log cache.
   */
  public void startWALFlushTimer() {
    pushTimer();
  }

  /**
   * Stops timer and records how much time was spent on flushing of data from write ahead log cache.
   */
  public void stopWALFlushTimer() {
    final long endTs = nanoTimer.getNano();
    final long timeDiff = (endTs - timeStamps.pop());

    if (walCountersHolder == null)
      walCountersHolder = new WALCountersHolder();

    walCountersHolder.flushCount++;
    walCountersHolder.flushTime += timeDiff;

    makeSnapshotIfNeeded(endTs);
  }

  private void checkComponentType(Component component, ComponentType expected) {
    if (!component.type.equals(expected))
      throw new IllegalStateException("Invalid component type , required " + expected + " but found " + component.type);
  }

  /**
   * Makes snapshot of data if time for next snapshot is passed. Also clear all data if {@link #cleanUpInterval} interval is over.
   *
   * @param currentTime Time of last measurement in nanoseconds or -1 if time is unknown
   */
  private void makeSnapshotIfNeeded(long currentTime) {
    if (currentTime < 0) {
      currentTime = nanoTimer.getNano();
    }

    if (lastSnapshotTimestamp == -1)
      lastSnapshotTimestamp = 0;

    if (lastSnapshotTimestamp < 0 || currentTime - lastSnapshotTimestamp >= intervalBetweenSnapshots) {
      snapshot = new PerformanceSnapshot(performanceCountersHolder, countersByComponent, writCacheCountersHolder,
          storageCountersHolder, walCountersHolder);
      lastSnapshotTimestamp = currentTime;
    }

    if (cleanUpInterval > 0) {
      if (currentTime - lastCleanUpTimeStamp >= cleanUpInterval) {
        performanceCountersHolder.clean();

        for (PerformanceCountersHolder pch : countersByComponent.values()) {
          pch.clean();
        }

        if (writCacheCountersHolder != null)
          writCacheCountersHolder.clean();

        if (storageCountersHolder != null)
          storageCountersHolder.clean();

        if (writCacheCountersHolder != null)
          walCountersHolder.clean();

        lastCleanUpTimeStamp = currentTime;
      }
    }
  }

  /**
   * Interface which is used by this tool to get current PC nano time. Implementation which calls <code>System.nanoTime()</code> is
   * used by default.
   */
  public interface NanoTimer {
    /**
     * @return Current PC nano time.
     */
    long getNano();
  }

  /**
   * Contains information about system component, name and count of operations in progress at current moment.
   */
  private static final class Component {
    private final String        name;
    private final ComponentType type;

    private int operationCount;

    Component(String name, ComponentType type) {
      this.name = name;
      this.type = type;

      operationCount = 1;
    }
  }

  public enum ComponentType {
    GENERAL {
      @Override
      PerformanceCountersHolder newCountersHolder() {
        return new PerformanceCountersHolder();
      }
    }, INDEX {
      @Override
      IndexCountersHolder newCountersHolder() {
        return new IndexCountersHolder();
      }
    }, CLUSTER {
      @Override
      OClusterCountersHolder newCountersHolder() {
        return new OClusterCountersHolder();
      }
    }, RIDBAG {
      @Override
      RidbagCountersHolder newCountersHolder() {
        return new RidbagCountersHolder();
      }
    };

    abstract PerformanceCountersHolder newCountersHolder();
  }

  /**
   * Contains data about write ahead log performance
   */
  public static class WALCountersHolder implements CountersHolder<WALCountersHolder> {
    /**
     * Amount of times WAL record was logged
     */
    private long logRecordCount;

    /**
     * Total time which was spent on logging of WAL records
     */
    private long logRecordTime;

    /**
     * Amount of times WAL "start atomic operation" record was logged
     */
    private long startRecordCount;

    /**
     * Total time which was spent on logging of "start atomic operation" records
     */
    private long startRecordTime;

    /**
     * Amount of times "stop atomic operation" record was logged
     */
    private long stopRecordCount;

    /**
     * Total time which was spent on logging of "stop atomic operation" records
     */
    private long stopRecordTime;

    /**
     * Amount of times WAL cache flush was called
     */
    private long flushCount;

    /**
     * Total time which was spent on flush of WAL cache
     */
    private long flushTime;

    @Override
    public void clean() {
      logRecordCount = 0;
      logRecordTime = 0;
      flushCount = 0;
      flushTime = 0;
    }

    @Override
    public void pushData(WALCountersHolder holder) {
      holder.logRecordCount += logRecordCount;
      holder.logRecordTime += logRecordTime;
      holder.flushCount += flushCount;
      holder.flushTime += flushTime;
    }

    /**
     * @return Average time which is spent on logging of single record or <code>-1</code> if value is undefined.
     */
    public long getLogTime() {
      if (logRecordCount == 0)
        return -1;

      return logRecordTime / logRecordCount;
    }

    /**
     * @return Average time which is spent on logging of "stop atomic operation" record or <code>-1</code> if value is undefined.
     */
    public long getStopAOTime() {
      if (stopRecordCount == 0)
        return -1;

      return stopRecordTime / stopRecordCount;
    }

    /**
     * @return Average time which is spent on logging of "start atomic operation" record or <code>-1</code> if value is undefined.
     */
    public long getStartAOTime() {
      if (startRecordCount == 0)
        return -1;

      return startRecordTime / startRecordCount;
    }

    /**
     * @return Average time which is spent on flush of WAL cache or <code>-1</code> if value is undefined.
     */
    public long getFlushTime() {
      if (flushCount == 0)
        return -1;

      return flushTime / flushCount;
    }

    @Override
    public ODocument toDocument() {
      final ODocument document = new ODocument();

      writeMetric(document, "logTime", getLogTime(), OType.LONG);
      writeMetric(document, "startAOTime", getStartAOTime(), OType.LONG);
      writeMetric(document, "stopAOTime", getStopAOTime(), OType.LONG);

      return document;
    }
  }

  /**
   * Contains data about storage performance
   */
  public static class StorageCountersHolder implements CountersHolder<StorageCountersHolder> {
    /**
     * Amount of times full checkpoint operation was performed.
     */
    private long fullCheckpointOperationsCount;

    /**
     * Total amount of time spent on full checkpoint operation.
     */
    private long fullCheckpointOperationsTime;

    @Override
    public void clean() {
      fullCheckpointOperationsCount = 0;
      fullCheckpointOperationsTime = 0;
    }

    @Override
    public void pushData(StorageCountersHolder holder) {
      holder.fullCheckpointOperationsCount += fullCheckpointOperationsCount;
      holder.fullCheckpointOperationsTime += fullCheckpointOperationsTime;
    }

    @Override
    public ODocument toDocument() {
      final ODocument document = new ODocument();
      writeMetric(document, "fullCheckpointTime", getFullCheckpointTime(), OType.LONG);

      return document;
    }

    /**
     * @return Average time which is spent on full checkpoint operation, or <code>-1</code> if value is undefined
     */
    public long getFullCheckpointTime() {
      if (fullCheckpointOperationsCount == 0)
        return -1;

      return fullCheckpointOperationsTime / fullCheckpointOperationsCount;
    }
  }

  /**
   * Contains write cache performance characteristics
   */
  public static class WritCacheCountersHolder implements CountersHolder<WritCacheCountersHolder> {
    /**
     * Count flush operation.
     */
    private long flushOperationsCount;

    /**
     * Sum of pages flushed in each write cache flush operation.
     */
    private long amountOfPagesFlushed;

    /**
     * Total time spent on all flush operations.
     */
    private long flushOperationsTime;

    /**
     * Count of all fuzzy checkpoints
     */
    private long fuzzyCheckpointCount;

    /**
     * Time is spent in all fuzzy checkpoints
     */
    private long fuzzyCheckpointTime;

    @Override
    public void clean() {
      flushOperationsCount = 0;
      amountOfPagesFlushed = 0;
      flushOperationsTime = 0;
      fuzzyCheckpointCount = 0;
      fuzzyCheckpointTime = 0;
    }

    @Override
    public void pushData(WritCacheCountersHolder holder) {
      holder.flushOperationsCount += flushOperationsCount;
      holder.amountOfPagesFlushed += amountOfPagesFlushed;
      holder.flushOperationsTime += flushOperationsTime;
      holder.fuzzyCheckpointCount += fuzzyCheckpointCount;
      holder.fuzzyCheckpointTime += fuzzyCheckpointTime;
    }

    /**
     * @return Average amount of pages which are flushed during "page flush" operation of write cache or <code>-1</code> if value is
     * undefined
     */
    public long getPagesPerFlush() {
      if (flushOperationsCount == 0)
        return -1;

      return amountOfPagesFlushed / flushOperationsCount;
    }

    /**
     * @return Average amount of time which is spent on each "page flush" operation of write cache or <code>-1</code> if value is
     * undefined.
     */
    public long getFlushOperationsTime() {
      if (flushOperationsCount == 0)
        return -1;

      return flushOperationsTime / flushOperationsCount;
    }

    /**
     * @return Average amount of time which is spent on "fuzzy checkpoint" operation or <code>-1</code> if value if undefined.
     */
    public long getFuzzyCheckpointTime() {
      if (fuzzyCheckpointCount == 0)
        return -1;

      return fuzzyCheckpointTime / fuzzyCheckpointCount;
    }

    @Override
    public ODocument toDocument() {
      final ODocument document = new ODocument();

      writeMetric(document, "pagesPerFlush", getPagesPerFlush(), OType.LONG);
      writeMetric(document, "flushOperationsTime", getFlushOperationsTime(), OType.LONG);
      writeMetric(document, "fuzzyCheckpointTime", getFuzzyCheckpointTime(), OType.LONG);

      return document;
    }
  }

  @SuppressWarnings("unused")
  public static class IndexCountersHolder extends PerformanceCountersHolder {
    private long updatedEntries;
    private long timeUpdateEntry;
    private long updateEntryPages;
    private long updateEntryFilePages;
    private long updateEntryPageTime;
    private long updateEntryFilePageTime;

    private long deletedEntries;
    private long timeDeleteEntry;
    private long deleteEntryPages;
    private long deleteEntryFilePages;
    private long deleteEntryPageTime;
    private long deleteEntryFilePageTime;

    private long readEntries;
    private long timeReadEntry;
    private long readEntryPages;

    private long readEntryFilePages;
    private long readEntryPageTime;
    private long readEntryFilePageTime;

    @Override
    public IndexCountersHolder newInstance() {
      return new IndexCountersHolder();
    }

    @Override
    public void clean() {
      super.clean();

      updatedEntries = 0;
      timeUpdateEntry = 0;
      updateEntryPages = 0;
      updateEntryFilePages = 0;
      updateEntryPageTime = 0;
      updateEntryFilePageTime = 0;

      deletedEntries = 0;
      timeDeleteEntry = 0;
      deleteEntryPages = 0;
      deleteEntryFilePages = 0;
      deleteEntryPageTime = 0;
      deleteEntryFilePageTime = 0;

      readEntries = 0;
      timeReadEntry = 0;
      readEntryPages = 0;
      readEntryFilePages = 0;
      readEntryPageTime = 0;
      readEntryFilePageTime = 0;
    }

    public long getUpdateEntryTime() {
      if (updatedEntries == 0)
        return -1;

      return timeUpdateEntry / updatedEntries;
    }

    public long getUpdateEntryPages() {
      if (updatedEntries == 0)
        return -1;

      return updateEntryPages / updatedEntries;
    }

    public long getUpdateEntryHitRate() {
      if (updateEntryPages == 0)
        return -1;

      return (int) ((100 * (updateEntryPages - updateEntryFilePages)) / updateEntryPages);
    }

    public long getUpdateEntryPageTime() {
      if (updateEntryPages == 0)
        return -1;

      return updateEntryPageTime / updateEntryPages;
    }

    public long getUpdateEntryFilePageTime() {
      if (updateEntryFilePages == 0)
        return -1;

      return updateEntryFilePageTime / updateEntryFilePages;
    }

    public long getDeleteEntryTime() {
      if (deletedEntries == 0)
        return -1;

      return timeDeleteEntry / deletedEntries;
    }

    public long getDeleteEntryPages() {
      if (deletedEntries == 0)
        return -1;

      return deleteEntryPages / deletedEntries;
    }

    public long getDeleteEntryHitRate() {
      if (deleteEntryPages == 0)
        return -1;

      return (int) ((100 * (deleteEntryPages - deleteEntryFilePages)) / deleteEntryPages);
    }

    public long getReadEntryTime() {
      if (readEntries == 0)
        return -1;

      return timeReadEntry / readEntries;
    }

    public long getReadEntryPages() {
      if (readEntries == 0)
        return -1;

      return readEntryPages / readEntries;
    }

    @Override
    public ODocument toDocument() {
      final ODocument document = super.toDocument();

      writeMetric(document, "updateEntryTime", getUpdateEntryTime(), OType.LONG);
      writeMetric(document, "updateEntryPages", getUpdateEntryPages(), OType.LONG);
      writeMetric(document, "deleteEntryTime", getDeleteEntryTime(), OType.LONG);
      writeMetric(document, "deleteEntryPages", getDeleteEntryPages(), OType.LONG);
      writeMetric(document, "readEntryTime", getReadEntryTime(), OType.LONG);
      writeMetric(document, "readEntryPages", getReadEntryPages(), OType.LONG);

      return document;
    }

    private class UpdateEntryOperation extends OOperation {
      @Override
      void incrementOperationsCounter(int pages, int filePages) {
        IndexCountersHolder.this.updateEntryPages += pages;
        IndexCountersHolder.this.updateEntryFilePages += filePages;
      }
    }

    private class DeleteEntryOperation extends OOperation {
      @Override
      void incrementOperationsCounter(int pages, int filePages) {
        IndexCountersHolder.this.deleteEntryPages += pages;
        IndexCountersHolder.this.deleteEntryFilePages += filePages;
      }
    }

    private class ReadEntryOperation extends OOperation {
      @Override
      void incrementOperationsCounter(int pages, int filePages) {
        IndexCountersHolder.this.readEntryPages += pages;
        IndexCountersHolder.this.readEntryFilePages += filePages;
      }
    }
  }

  @SuppressWarnings("unused")
  public static class RidbagCountersHolder extends PerformanceCountersHolder {
    private long updatedEntries;
    private long timeUpdateEntry;
    private long updateEntryPages;
    private long updateEntryFilePages;

    private long deletedEntries;
    private long timeDeleteEntry;
    private long deleteEntryPages;
    private long deleteEntryFilePages;

    private long readEntries;
    private long timeReadEntry;
    private long readEntryPages;
    private long readEntryFilePages;

    private long loads;
    private long loadTime;
    private long loadPages;
    private long loadFilePages;

    @Override
    public PerformanceCountersHolder newInstance() {
      return new RidbagCountersHolder();
    }

    @Override
    public void clean() {
      super.clean();

      updatedEntries = 0;
      timeUpdateEntry = 0;
      updateEntryPages = 0;
      updateEntryFilePages = 0;

      deletedEntries = 0;
      timeDeleteEntry = 0;
      deleteEntryPages = 0;
      deleteEntryFilePages = 0;

      readEntries = 0;
      timeReadEntry = 0;
      readEntryPages = 0;
      readEntryFilePages = 0;

      loads = 0;
      loadTime = 0;
      loadPages = 0;
      loadFilePages = 0;
    }

    public long getUpdateEntryTime() {
      if (updatedEntries == 0)
        return -1;

      return timeUpdateEntry / updatedEntries;
    }

    public long getUpdateEntryPages() {
      if (updatedEntries == 0)
        return -1;

      return updateEntryPages / updatedEntries;
    }

    public long getDeleteEntryTime() {
      if (deletedEntries == 0)
        return -1;

      return timeDeleteEntry / deletedEntries;
    }

    public long getDeleteEntryPages() {
      if (deletedEntries == 0)
        return -1;

      return deleteEntryPages / deletedEntries;
    }

    public long getReadEntryTime() {
      if (timeReadEntry == 0)
        return -1;

      return timeReadEntry / readEntries;
    }

    public long getReadEntryPages() {
      if (readEntries == 0)
        return -1;

      return readEntryPages / readEntries;
    }

    public long getLoadTime() {
      return loadTime / loads;
    }

    public long getLoadPages() {
      if (loads == 0)
        return -1;

      return loadPages / loads;
    }

    @Override
    public ODocument toDocument() {
      final ODocument document = super.toDocument();

      writeMetric(document, "updateEntryTime", getUpdateEntryTime(), OType.LONG);
      writeMetric(document, "updateEntryPages", getUpdateEntryPages(), OType.LONG);
      writeMetric(document, "deleteEntryTime", getDeleteEntryTime(), OType.LONG);
      writeMetric(document, "deleteEntryPages", getDeleteEntryPages(), OType.LONG);
      writeMetric(document, "readEntryTime", getReadEntryTime(), OType.LONG);
      writeMetric(document, "readEntryPages", getReadEntryPages(), OType.LONG);
      writeMetric(document, "loadTime", getLoadTime(), OType.LONG);
      writeMetric(document, "loadPages", getLoadPages(), OType.LONG);

      return document;
    }

    private class UpdateEntryOperation extends OOperation {
      @Override
      void incrementOperationsCounter(int pages, int filePages) {
        RidbagCountersHolder.this.updateEntryPages += pages;
        RidbagCountersHolder.this.updateEntryFilePages += filePages;
      }
    }

    private class DeleteEntryPages extends OOperation {
      @Override
      void incrementOperationsCounter(int pages, int filePages) {
        RidbagCountersHolder.this.deleteEntryPages += pages;
        RidbagCountersHolder.this.deleteEntryFilePages += filePages;
      }
    }

    private class ReadEntryPages extends OOperation {
      @Override
      void incrementOperationsCounter(int pages, int filePages) {
        RidbagCountersHolder.this.readEntryPages += pages;
        RidbagCountersHolder.this.readEntryFilePages += filePages;
      }
    }

    private class LoadPages extends OOperation {
      @Override
      void incrementOperationsCounter(int pages, int filePages) {
        RidbagCountersHolder.this.loadPages += pages;
        RidbagCountersHolder.this.loadFilePages += filePages;
      }
    }
  }

  /**
   * Container for all performance counters which are shared between durable components and whole system.
   */
  public static class PerformanceCountersHolder implements CountersHolder<PerformanceCountersHolder> {
    /**
     * Amount of times when atomic operation commit was performed.
     */
    private long commitCount = 0;

    /**
     * Summary time which was spent on atomic operation commits.
     */
    private long commitTime = 0;

    /**
     * Amount of operations performed by related component.
     */
    private long operationsCount = 0;

    /**
     * Amount of times when cache was accessed during the session.
     */
    private long cacheAccessCount = 0;

    /**
     * Amount of "cache hit" times during the session.
     */
    private long cacheHit = 0;

    /**
     * Summary time which was spent on access of pages from file system.
     */
    private long pageReadFromFileTime = 0;

    /**
     * Amount of pages in total which were accessed from file system.
     */
    private long pageReadFromFileCount = 0;

    /**
     * Summary time which was spent on access of pages from disk cache.
     */
    private long pageReadFromCacheTime = 0;

    /**
     * Amount of pages in total which were accessed from disk cache.
     */
    private long pageReadFromCacheCount = 0;

    /**
     * Summary time which was spent to write pages to disk cache.
     */
    private long pageWriteToCacheTime = 0;

    /**
     * Amount of pages in total which were written to disk cache.
     */
    private long pageWriteToCacheCount = 0;

    @SuppressWarnings("unused")
    private OOperation currentOperation;

    /**
     * Clears all performance data.
     */
    @Override
    public void clean() {
      commitCount = 0;
      commitTime = 0;
      operationsCount = 0;
      cacheAccessCount = 0;
      cacheHit = 0;
      pageReadFromFileTime = 0;
      pageReadFromFileCount = 0;
      pageReadFromCacheTime = 0;
      pageReadFromCacheCount = 0;
      pageWriteToCacheTime = 0;
      pageWriteToCacheCount = 0;
    }

    /**
     * Aggregates passed in and current performance data. Result is put in passed in data.
     *
     * @param aggregator Data to aggregate
     */
    @Override
    public void pushData(PerformanceCountersHolder aggregator) {
      aggregator.operationsCount += operationsCount;
      aggregator.cacheAccessCount += cacheAccessCount;
      aggregator.cacheHit += cacheHit;
      aggregator.pageReadFromFileTime += pageReadFromFileTime;
      aggregator.pageReadFromFileCount += pageReadFromFileCount;
      aggregator.pageReadFromCacheTime += pageReadFromCacheTime;
      aggregator.pageReadFromCacheCount += pageReadFromCacheCount;
      aggregator.pageWriteToCacheTime += pageWriteToCacheTime;
      aggregator.pageWriteToCacheCount += pageWriteToCacheCount;
      aggregator.commitTime += commitTime;
      aggregator.commitCount += commitCount;
    }

    /**
     * @return Average time is spent on commit of single atomic operation.
     */
    public long getCommitTime() {
      if (commitCount == 0)
        return -1;

      return commitTime / commitCount;
    }

    /**
     * @return Read speed of data in pages per second on cache level or value which is less than 0, which means that value cannot be
     * calculated.
     */
    public long getReadSpeedFromCacheInPages() {
      if (pageReadFromCacheTime == 0)
        return -1;

      return (pageReadFromCacheCount * NANOS_IN_SECOND) / pageReadFromCacheTime;
    }

    /**
     * @return Read speed of data on file system level in pages per second or value which is less than 0, which means that value
     * cannot be calculated.
     */
    public long getReadSpeedFromFileInPages() {
      if (pageReadFromFileTime == 0)
        return -1;

      return (pageReadFromFileCount * NANOS_IN_SECOND) / pageReadFromFileTime;
    }

    /**
     * @return Amount of pages read from cache in total.
     */
    public long getAmountOfPagesReadFromCache() {
      return pageReadFromCacheCount;
    }

    /**
     * @return Amount of pages are read from file.
     */
    public long getAmountOfPagesReadFromFile() {
      return pageReadFromFileCount;
    }

    /**
     * @return Write speed of data in pages per second on cache level or value which is less than 0, which means that value cannot
     * be calculated.
     */
    public long getWriteSpeedInCacheInPages() {
      if (pageWriteToCacheTime == 0)
        return -1;

      return (pageWriteToCacheCount * NANOS_IN_SECOND) / pageWriteToCacheTime;
    }

    /**
     * @return Amount of pages written to cache.
     */
    public long getAmountOfPagesWrittenInCache() {
      return pageWriteToCacheCount;
    }

    /**
     * @return Percent of cache hits or value which is less than 0, which means that value cannot be calculated.
     */
    public int getCacheHits() {
      if (cacheAccessCount == 0)
        return -1;

      return (int) ((cacheHit * 100) / cacheAccessCount);
    }

    /**
     * @return Amount of pages accessed from cache for each component operation
     */
    public long getAmountOfPagesPerOperation() {
      if (operationsCount == 0)
        return -1;

      return pageReadFromCacheCount / operationsCount;
    }

    /**
     * Converts properties of given class into values of fields of returned document. Names of fields equal to names of properties.
     *
     * @return Performance characteristics of storage.
     */
    @Override
    public ODocument toDocument() {
      final ODocument document = new ODocument();

      writeMetric(document, "readSpeedFromCacheInPages", getReadSpeedFromCacheInPages(), OType.LONG);
      writeMetric(document, "readSpeedFromFileInPages", getReadSpeedFromFileInPages(), OType.LONG);
      writeMetric(document, "amountOfPagesReadFromCache", getAmountOfPagesReadFromCache(), OType.LONG);
      writeMetric(document, "writeSpeedInCacheInPages", getWriteSpeedInCacheInPages(), OType.LONG);
      writeMetric(document, "amountOfPagesWrittenInCache", getAmountOfPagesWrittenInCache(), OType.LONG);
      writeMetric(document, "amountOfPagesReadFromFile", getAmountOfPagesReadFromFile(), OType.LONG);
      writeMetric(document, "cacheHits", getCacheHits(), OType.INTEGER);
      writeMetric(document, "amountOfPagesPerOperation", getAmountOfPagesPerOperation(), OType.LONG);
      writeMetric(document, "commitTime", getCommitTime(), OType.LONG);

      return document;
    }

    public PerformanceCountersHolder newInstance() {
      return new PerformanceCountersHolder();
    }
  }

  /**
   * Snapshot of all performance data of current container.
   */
  public static final class PerformanceSnapshot {
    public final PerformanceCountersHolder performanceCountersHolder;
    public final WritCacheCountersHolder   writCacheCountersHolder;
    public final StorageCountersHolder     storageCountersHolder;
    public final WALCountersHolder         walCountersHolder;

    public final Map<String, PerformanceCountersHolder> countersByComponent;

    /**
     * Makes snapshot of performance data and stores them in final fields.
     */
    PerformanceSnapshot(PerformanceCountersHolder performanceCountersHolder,
        Map<String, PerformanceCountersHolder> countersByComponent, WritCacheCountersHolder writCacheCountersHolder,
        StorageCountersHolder storageCountersHolder, WALCountersHolder walCountersHolder) {

      this.performanceCountersHolder = performanceCountersHolder.newInstance();
      performanceCountersHolder.pushData(this.performanceCountersHolder);

      // to preserve thread safety we assign map instance to the final field when it is completely filled by data
      Map<String, PerformanceCountersHolder> counters = new HashMap<>();
      for (Map.Entry<String, PerformanceCountersHolder> entry : countersByComponent.entrySet()) {
        final PerformanceCountersHolder holder = entry.getValue().newInstance();
        entry.getValue().pushData(holder);

        counters.put(entry.getKey(), holder);
      }

      this.countersByComponent = counters;

      if (writCacheCountersHolder != null) {
        final WritCacheCountersHolder wh = new WritCacheCountersHolder();
        writCacheCountersHolder.pushData(wh);
        this.writCacheCountersHolder = wh;
      } else {
        this.writCacheCountersHolder = null;
      }

      if (storageCountersHolder != null) {
        final StorageCountersHolder sch = new StorageCountersHolder();
        storageCountersHolder.pushData(sch);
        this.storageCountersHolder = sch;
      } else {
        this.storageCountersHolder = null;
      }

      if (walCountersHolder != null) {
        final WALCountersHolder wch = new WALCountersHolder();
        walCountersHolder.pushData(wch);
        this.walCountersHolder = wch;
      } else {
        this.walCountersHolder = null;
      }
    }

  }

  /**
   * Common interface which should implement all classes which contain system/component performance data.
   *
   * @param <T> Real component class
   */
  public interface CountersHolder<T extends CountersHolder> {
    /**
     * Resets all performance characteristics to default values.
     */
    void clean();

    /**
     * Accumulates data in current and passed in containers and push data back to passed in container.
     */
    void pushData(T holder);

    /**
     * Serializes performance data as values of fields of returned document.
     *
     * @return Document filled with performance data
     */
    ODocument toDocument();
  }

  public static void writeMetric(final ODocument document, final String metricName, final Number metricValue) {
    writeMetric(document, metricName, metricValue, OType.LONG);
  }

  public static void writeMetric(final ODocument document, final String metricName, final Number metricValue,
      final OType metricType) {
    if (metricValue.longValue() != -1)
      document.field(metricName, metricValue, metricType);
  }
}
