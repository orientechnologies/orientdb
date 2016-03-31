/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
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
 * accept any parameters, they return performance data on system level and, other kind accept component name, they return performance
 * data gathered on separate component or on system level if null is passed as method name. If data from component with passed in
 * name is absent then -1 is returned.
 * <p>
 * At the moment all performance data are gathered both for durable components and whole system with exception of
 * {@link #getCommitTimeAvg()}.
 * <p>
 * This container may be used both for gathering information in single thread and in mutlithreading environment.
 * Container itself is not thread safe, but it supports snapshoting of data.
 * <p>
 * Every time when new data are gathered by container it checks whether minimum interval between two snapshots is passed
 * and makes data snapshot if needed, data snapshot can be used by aggregator which merges statistics from all
 * containers together. Only last snapshot is hold by container.
 * <p>
 * Also container support time series functionality by clearing of previous statics every time when snapshot is done.
 *
 * @author Andrey Lomakin
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
  private final Deque<Long> timeStamps = new ArrayDeque<Long>();

  /**
   * Stack of active components or in another words components which currently perform actions inside current thread.
   */
  private Deque<Component> componentsStack = new ArrayDeque<Component>();

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
   * Indicates whether all gathered statistic should be cleared before snapshot is taken
   */
  private final boolean cleanOnSnapshot;

  /**
   * Time stamp of last snapshot is taken by container.
   */
  private long lastSnapshotTimestamp = -1;

  /**
   * Snapshot of all data measured during session.
   */
  private volatile PerformanceSnapshot snapshot;

  /**
   * Map containing performance counters specific for concrete software component.
   */
  private Map<String, PerformanceCountersHolder> countersByComponent = new HashMap<String, PerformanceCountersHolder>();

  /**
   * @param intervalBetweenSnapshots Minimum interval between two snapshots taken by container.
   * @param cleanOnSnapshot          Whether all data should be cleared after snapshot is taken.
   */
  public OSessionStoragePerformanceStatistic(long intervalBetweenSnapshots, boolean cleanOnSnapshot) {
    this(intervalBetweenSnapshots, new NanoTimer() {
      @Override
      public long getNano() {
        return System.nanoTime();
      }
    }, cleanOnSnapshot);
  }

  /**
   * @param nanoTimer                Service to get current value of PC nano time.
   * @param intervalBetweenSnapshots Minimum interval between two snapshots taken by container.
   * @param cleanOnSnapshot          Whether all data should be cleared after snapshot is taken.
   */
  public OSessionStoragePerformanceStatistic(long intervalBetweenSnapshots, NanoTimer nanoTimer, boolean cleanOnSnapshot) {
    this.nanoTimer = nanoTimer;
    this.intervalBetweenSnapshots = intervalBetweenSnapshots;
    this.performanceCountersHolder = new PerformanceCountersHolder();
    this.cleanOnSnapshot = cleanOnSnapshot;
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
  public void startComponentOperation(String componentName) {
    final Component currentComponent = componentsStack.peek();

    if (currentComponent != null && componentName.equals(currentComponent.name)) {
      currentComponent.operationCount++;
      return;
    }

    componentsStack.push(new Component(componentName));
  }

  /**
   * Indicates that the most earliest component in stack of components has completed it's operation, so performance data
   * for this component is stopped to be gathered.
   *
   * @see #startComponentOperation(String)
   */
  public void completeComponentOperation() {
    final Component currentComponent = componentsStack.peek();
    if (currentComponent == null)
      return;

    currentComponent.operationCount--;

    if (currentComponent.operationCount == 0) {
      final String componentName = currentComponent.name;

      PerformanceCountersHolder cHolder = countersByComponent.get(componentName);
      if (cHolder == null) {
        cHolder = new PerformanceCountersHolder();
        countersByComponent.put(componentName, cHolder);
      }

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
   * @return Average time of commit of atomic operation in nanoseconds or value which is less than 0, which means that value cannot
   * be calculated.
   */
  public long getCommitTimeAvg() {
    return performanceCountersHolder.getCommitTimeAvg();
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
   * Takes performance data are split by components from last snapshot and aggregates them with data passed inside method as parameter.
   * Result of aggregation of performance data is returned inside of passed in performance data.
   *
   * @param counters Performance data for each component.
   */
  public void pushComponentCounters(Map<String, PerformanceCountersHolder> counters) {
    if (snapshot == null)
      return;

    for (Map.Entry<String, PerformanceCountersHolder> entry : snapshot.countersByComponent.entrySet()) {
      final String componentName = entry.getKey();

      PerformanceCountersHolder holder = counters.get(componentName);
      if (holder == null) {
        holder = new PerformanceCountersHolder();
        counters.put(componentName, holder);
      }

      entry.getValue().pushData(holder);
    }
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
   * Takes performance data for component from last snapshot and aggregates them with data passed inside method as parameter.
   * Result of aggregation of performance data is returned inside of passed in performance data.
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
   *
   * @return Performance characteristics of storage gathered after call of
   * {@link OAbstractPaginatedStorage#startGatheringPerformanceStatisticForCurrentThread()}
   */
  public ODocument toDocument() {
    final ODocument document = performanceCountersHolder.toDocument();

    document.field("commitTimeAvg", getCommitTimeAvg(), OType.LONG);

    final Map<String, ODocument> countersMap = new HashMap<String, ODocument>();
    for (Map.Entry<String, PerformanceCountersHolder> entry : countersByComponent.entrySet()) {
      countersMap.put(entry.getKey(), entry.getValue().toDocument());
    }

    document.field("dataByComponent", countersMap, OType.EMBEDDEDMAP);

    return document;
  }

  /**
   * Increments counter of page accesses from cache.
   * <p>
   * If you wish to gather statistic for current durable component please call {@link #startComponentOperation(String)} method
   * before the call and {@link #completeComponentOperation()} after the call.
   */
  public void incrementPageAccessOnCacheLevel(boolean cacheHit) {
    performanceCountersHolder.cacheAccessCount++;
    if (cacheHit)
      performanceCountersHolder.cacheHit++;

    for (Component component : componentsStack) {
      final String componentName = component.name;

      PerformanceCountersHolder cHolder = countersByComponent.get(componentName);
      if (cHolder == null) {
        cHolder = new PerformanceCountersHolder();
        countersByComponent.put(componentName, cHolder);
      }

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
    timeStamps.push(nanoTimer.getNano());
  }

  /**
   * Stops and records results of timer which counts how much time was spent on read of page from file system.
   * <p>
   * If you wish to gather statistic for current durable component please call {@link #startComponentOperation(String)} method
   * before the call and {@link #completeComponentOperation()} after the call.
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

      PerformanceCountersHolder cHolder = countersByComponent.get(componentName);
      if (cHolder == null) {
        cHolder = new PerformanceCountersHolder();
        countersByComponent.put(componentName, cHolder);
      }
      cHolder.pageReadFromFileTime += timeDiff;
      cHolder.pageReadFromFileCount += readPages;
    }

    makeSnapshotIfNeeded(endTs);
  }

  /**
   * Starts timer which counts how much time was spent on read of page from disk cache.
   */
  public void startPageReadFromCacheTimer() {
    timeStamps.push(nanoTimer.getNano());
  }

  /**
   * Stops and records results of timer which counts how much time was spent on read of page from disk cache.
   * <p>
   * If you wish to gather statistic for current durable component please call {@link #startComponentOperation(String)} method
   * before the call and {@link #completeComponentOperation()} after the call.
   */
  public void stopPageReadFromCacheTimer() {
    final long endTs = nanoTimer.getNano();
    final long timeDiff = (endTs - timeStamps.pop());

    performanceCountersHolder.pageReadFromCacheTime += timeDiff;
    performanceCountersHolder.pageReadFromCacheCount++;

    for (Component component : componentsStack) {
      final String componentName = component.name;
      PerformanceCountersHolder cHolder = countersByComponent.get(componentName);
      if (cHolder == null) {
        cHolder = new PerformanceCountersHolder();
        countersByComponent.put(componentName, cHolder);
      }

      cHolder.pageReadFromCacheTime += timeDiff;
      cHolder.pageReadFromCacheCount++;
    }

    makeSnapshotIfNeeded(endTs);
  }

  /**
   * Starts timer which counts how much time was spent on write of page to disk cache.
   */
  public void startPageWriteInCacheTimer() {
    timeStamps.push(nanoTimer.getNano());
  }

  /**
   * Stops and records results of timer which counts how much time was spent to write page to disk cache.
   * <p>
   * If you wish to gather statistic for current durable component please call {@link #startComponentOperation(String)} method
   * before the call and {@link #completeComponentOperation()} after the call.
   */
  public void stopPageWriteInCacheTimer() {
    final long endTs = nanoTimer.getNano();
    final long timeDiff = (endTs - timeStamps.pop());

    performanceCountersHolder.pageWriteToCacheTime += timeDiff;
    performanceCountersHolder.pageWriteToCacheCount++;

    for (Component component : componentsStack) {
      final String componentName = component.name;
      PerformanceCountersHolder cHolder = countersByComponent.get(componentName);
      if (cHolder == null) {
        cHolder = new PerformanceCountersHolder();
        countersByComponent.put(componentName, cHolder);
      }

      cHolder.pageWriteToCacheTime += timeDiff;
      cHolder.pageWriteToCacheCount++;
    }

    makeSnapshotIfNeeded(endTs);
  }

  /**
   * Starts timer which counts how much time was spent on atomic operation commit.
   */
  public void startCommitTimer() {
    timeStamps.push(nanoTimer.getNano());
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
   * Makes snapshot of data if time for next snapshot is passed.
   * Also clear all data if related flag is specified during construction of object.
   *
   * @param currentTime Time of last measurement or -1 if time is unknown
   */
  private void makeSnapshotIfNeeded(long currentTime) {
    if (currentTime < 0) {
      currentTime = nanoTimer.getNano();
    }

    if (lastSnapshotTimestamp == -1)
      lastSnapshotTimestamp = 0;

    if (lastSnapshotTimestamp < 0 || currentTime - lastSnapshotTimestamp >= intervalBetweenSnapshots) {
      snapshot = new PerformanceSnapshot(performanceCountersHolder, countersByComponent);
      lastSnapshotTimestamp = currentTime;

      if (cleanOnSnapshot) {
        performanceCountersHolder.clean();

        for (PerformanceCountersHolder pch : countersByComponent.values()) {
          pch.clean();
        }
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
   * Holds information about system component, name and count of operations in progress at current moment.
   */
  private static final class Component {
    private final String name;

    private int operationCount;

    Component(String name) {
      this.name = name;
      operationCount = 1;
    }
  }

  /**
   * Container for all performance counters which are shared between durable components and whole system.
   */
  public static final class PerformanceCountersHolder {
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

    /**
     * Clears all performance data.
     */
    private void clean() {
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
    public long getCommitTimeAvg() {
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
    public ODocument toDocument() {
      final ODocument document = new ODocument();

      document.field("readSpeedFromCacheInPages", getReadSpeedFromCacheInPages(), OType.LONG);
      document.field("readSpeedFromFileInPages", getReadSpeedFromFileInPages(), OType.LONG);
      document.field("amountOfPagesReadFromCache", getAmountOfPagesReadFromCache(), OType.LONG);
      document.field("writeSpeedInCacheInPages", getWriteSpeedInCacheInPages(), OType.LONG);
      document.field("amountOfPagesWrittenInCache", getAmountOfPagesWrittenInCache(), OType.LONG);
      document.field("amountOfPagesReadFromFile", getAmountOfPagesReadFromFile(), OType.LONG);
      document.field("cacheHits", getCacheHits(), OType.INTEGER);
      document.field("amountOfPagesPerOperation", getAmountOfPagesPerOperation(), OType.LONG);

      return document;
    }
  }

  /**
   * Snapshot of all performance data of current container.
   */
  private final static class PerformanceSnapshot {
    private final PerformanceCountersHolder              performanceCountersHolder;
    private final Map<String, PerformanceCountersHolder> countersByComponent;

    PerformanceSnapshot(PerformanceCountersHolder performanceCountersHolder,
        Map<String, PerformanceCountersHolder> countersByComponent) {
      this.performanceCountersHolder = new PerformanceCountersHolder();
      performanceCountersHolder.pushData(this.performanceCountersHolder);

      //to preserve thread safety we assign map instance to the final field when it is completely filled by data
      Map<String, PerformanceCountersHolder> counters = new HashMap<String, PerformanceCountersHolder>();
      for (Map.Entry<String, PerformanceCountersHolder> entry : countersByComponent.entrySet()) {
        final PerformanceCountersHolder holder = new PerformanceCountersHolder();
        entry.getValue().pushData(holder);

        counters.put(entry.getKey(), holder);
      }

      this.countersByComponent = counters;
    }

  }

}
