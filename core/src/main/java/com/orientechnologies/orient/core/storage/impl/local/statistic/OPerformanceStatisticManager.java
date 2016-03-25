package com.orientechnologies.orient.core.storage.impl.local.statistic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.orientechnologies.orient.core.storage.impl.local.statistic.OSessionStoragePerformanceStatistic.PerformanceCountersHolder;

public class OPerformanceStatisticManager {
  private final long intervalBetweenSnapshots;
  private volatile boolean              enabled                 = false;
  private final    ThreadLocal<Boolean> enabledForCurrentThread = new ThreadLocal<Boolean>() {
    @Override
    protected Boolean initialValue() {
      return false;
    }
  };

  private final ConcurrentHashMap<Thread, OSessionStoragePerformanceStatistic> statistics = new ConcurrentHashMap<Thread, OSessionStoragePerformanceStatistic>();

  private volatile DeadThreadsStatistic deadThreadsStatistic;
  private final AtomicBoolean deadThreadsUpdateIsBusy = new AtomicBoolean();

  public OPerformanceStatisticManager(long intervalBetweenSnapshots) {
    this.intervalBetweenSnapshots = intervalBetweenSnapshots;
  }

  public void startThreadMonitoring() {
    enabledForCurrentThread.set(true);
  }

  public OSessionStoragePerformanceStatistic stopThreadMonitoring() {
    enabledForCurrentThread.set(false);

    final Thread currentThread = Thread.currentThread();
    return statistics.get(currentThread);
  }

  public void startMonitoring() {
    enabled = true;
  }

  public void stopMonitoring() {
    enabled = false;
  }

  public OSessionStoragePerformanceStatistic getSessionPerformanceStatistic() {
    if (!enabled && !enabledForCurrentThread.get())
      return null;

    final Thread currentThread = Thread.currentThread();
    OSessionStoragePerformanceStatistic performanceStatistic = statistics.get(currentThread);

    if (performanceStatistic != null) {
      return performanceStatistic;
    }

    performanceStatistic = new OSessionStoragePerformanceStatistic(intervalBetweenSnapshots);

    final OSessionStoragePerformanceStatistic oldStatistic = statistics.putIfAbsent(currentThread, performanceStatistic);
    if (oldStatistic != null) {
      performanceStatistic = oldStatistic;
    }

    return performanceStatistic;
  }

  public long getAmountOfPagesPerOperation(String componentName) {
    final PerformanceCountersHolder componentCountersHolder = new PerformanceCountersHolder();
    fetchComponentCounters(componentName, componentCountersHolder);
    return componentCountersHolder.getAmountOfPagesPerOperation();
  }

  public int getCacheHits() {
    final PerformanceCountersHolder countersHolder = new PerformanceCountersHolder();
    fetchSystemCounters(countersHolder);
    return countersHolder.getCacheHits();
  }

  public int getCacheHits(String componentName) {
    final PerformanceCountersHolder countersHolder = new PerformanceCountersHolder();
    fetchComponentCounters(componentName, countersHolder);
    return countersHolder.getCacheHits();
  }

  public long getCommitTimeAvg() {
    final PerformanceCountersHolder countersHolder = new PerformanceCountersHolder();
    fetchSystemCounters(countersHolder);
    return countersHolder.getCommitTimeAvg();
  }

  public long getReadSpeedFromCacheInPages() {
    final PerformanceCountersHolder countersHolder = new PerformanceCountersHolder();
    fetchSystemCounters(countersHolder);
    return countersHolder.getReadSpeedFromCacheInPages();
  }

  public long getReadSpeedFromCacheInPages(String componentName) {
    final PerformanceCountersHolder countersHolder = new PerformanceCountersHolder();
    fetchComponentCounters(componentName, countersHolder);
    return countersHolder.getReadSpeedFromCacheInPages();
  }

  public long getReadSpeedFromFileInPages() {
    final PerformanceCountersHolder countersHolder = new PerformanceCountersHolder();
    fetchSystemCounters(countersHolder);
    return countersHolder.getReadSpeedFromFileInPages();
  }

  public long getReadSpeedFromFileInPages(String componentName) {
    final PerformanceCountersHolder countersHolder = new PerformanceCountersHolder();
    fetchComponentCounters(componentName, countersHolder);
    return countersHolder.getReadSpeedFromFileInPages();
  }

  public long getWriteSpeedInCacheInPages() {
    final PerformanceCountersHolder countersHolder = new PerformanceCountersHolder();
    fetchSystemCounters(countersHolder);
    return countersHolder.getWriteSpeedInCacheInPages();
  }

  public long getWriteSpeedInCacheInPages(String componentName) {
    final PerformanceCountersHolder countersHolder = new PerformanceCountersHolder();
    fetchComponentCounters(componentName, countersHolder);
    return countersHolder.getWriteSpeedInCacheInPages();
  }

  private void fetchSystemCounters(PerformanceCountersHolder countersHolder) {
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

    final DeadThreadsStatistic ds = deadThreadsStatistic;
    final PerformanceCountersHolder dch = ds.countersHolderForDeadThreads;
    dch.pushData(countersHolder);
  }

  private void fetchComponentCounters(String componentName, PerformanceCountersHolder componentCountersHolder) {
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

    final DeadThreadsStatistic ds = deadThreadsStatistic;
    final PerformanceCountersHolder dch = ds.countersByComponentsForDeadThreads.get(componentName);
    if (dch != null) {
      dch.pushData(componentCountersHolder);
    }
  }

  private void updateDeadThreadsStatistic(ArrayList<Thread> threadsToRemove) {
    if (!deadThreadsUpdateIsBusy.get() && deadThreadsUpdateIsBusy.compareAndSet(false, true)) {
      try {
        final DeadThreadsStatistic newDS = new DeadThreadsStatistic();
        final DeadThreadsStatistic oldDS = deadThreadsStatistic;

        if (oldDS != null) {
          oldDS.countersHolderForDeadThreads.pushData(newDS.countersHolderForDeadThreads);

          for (Map.Entry<String, PerformanceCountersHolder> oldEntry : oldDS.countersByComponentsForDeadThreads.entrySet()) {
            final PerformanceCountersHolder holder = new PerformanceCountersHolder();
            oldEntry.getValue().pushData(holder);

            newDS.countersByComponentsForDeadThreads.put(oldEntry.getKey(), holder);
          }
        }

        for (Thread deadThread : threadsToRemove) {
          final OSessionStoragePerformanceStatistic sessionStoragePerformanceStatistic = statistics.remove(deadThread);
          assert sessionStoragePerformanceStatistic != null;

          sessionStoragePerformanceStatistic.pushSystemCounters(newDS.countersHolderForDeadThreads);
          sessionStoragePerformanceStatistic.pushComponentCounters(newDS.countersByComponentsForDeadThreads);
        }

        deadThreadsStatistic = newDS;
      } finally {
        deadThreadsUpdateIsBusy.set(false);
      }
    } else {
      while (!deadThreadsUpdateIsBusy.get()) {
        Thread.yield();
      }
    }
  }

  private final class DeadThreadsStatistic {
    final PerformanceCountersHolder              countersHolderForDeadThreads       = new PerformanceCountersHolder();
    final Map<String, PerformanceCountersHolder> countersByComponentsForDeadThreads = new HashMap<String, PerformanceCountersHolder>();
  }
}
