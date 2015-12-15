package com.orientechnologies.orient.core.storage.impl.local.statistic;

public interface OStoragePerformanceStatisticMXBean {
  void startMeasurement();

  void stopMeasurement();

  long getReadSpeedFromCacheInMB();

  long getReadSpeedFromCacheInPages();

  long getReadSpeedFromFileInPages();

  long getReadSpeedFromFileInMB();

  long getAmountOfPagesReadFromCache();

  long getAmountOfPagesReadFromFileSystem();

  long getWriteSpeedInCacheInPages();

  long getWriteSpeedInCacheInMB();

  long getWriteSpeedInFileInPages();

  long getWriteSpeedInFileInMB();

  long getAmountOfPagesWrittenToCache();

  long getCommitTimeAvg();

  int getCacheHits();
}
