package com.orientechnologies.orient.core.storage.impl.local.statistic;

import java.util.Set;
import java.util.logging.StreamHandler;

/**
 * Interface to expose {@link OPerformanceStatisticManager} methods through JMX.
 */
public interface OPerformanceStatisticManagerMXBean {
  void startMonitoring();

  void stopMonitoring();

  OSessionStoragePerformanceStatistic getSessionPerformanceStatistic();

  long getAmountOfPagesPerOperation(String componentName);

  int getCacheHits();

  int getCacheHits(String componentName);

  long getCommitTimeAvg();

  long getReadSpeedFromCacheInPages();

  long getReadSpeedFromCacheInPages(String componentName);

  long getReadSpeedFromFileInPages();

  long getReadSpeedFromFileInPages(String componentName);

  long getWriteSpeedInCacheInPages();

  long getWriteSpeedInCacheInPages(String componentName);

  Set<String> getComponentNames();
}
