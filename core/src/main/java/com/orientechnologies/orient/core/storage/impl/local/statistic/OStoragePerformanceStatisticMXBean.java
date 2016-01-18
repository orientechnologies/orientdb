package com.orientechnologies.orient.core.storage.impl.local.statistic;

/**
 * Interface for JMX bean which exposes methods and properties of
 * {@link OStoragePerformanceStatistic}
 */
public interface OStoragePerformanceStatisticMXBean {
  /**
   * Starts gathering of performance statistic for storage.
   */
  void startMeasurement();

  /**
   * Stops gathering of performance statistic for storage, but does not clear snapshot values.
   */
  void stopMeasurement();

  /**
   * @return <code>true</code> if statistic is measured inside of storage.
   */
  boolean isMeasurementEnabled();

  /**
   * @return Read speed of data in megabytes per second on cache level
   * or value which is less than 0, which means that value can not be calculated.
   */
  long getReadSpeedFromCacheInMB();

  /**
   * @return Read speed of data in pages per second on cache level
   * or value which is less than 0, which means that value can not be calculated.
   */
  long getReadSpeedFromCacheInPages();

  /**
   * @return Read speed of data on file system level in pages per second
   * or value which is less than 0, which means that value can not be calculated.
   */
  long getReadSpeedFromFileInPages();

  /**
   * @return Read speed of data on file system level in megabytes per second
   * or value which is less than 0, which means that value can not be calculated.
   */
  long getReadSpeedFromFileInMB();

  /**
   * @return Write speed of data in pages per second on cache level
   * or value which is less than 0, which means that value can not be calculated.
   */
  long getWriteSpeedInCacheInPages();

  /**
   * @return Write speed of data in megabytes per second on cache level
   * or value which is less than 0, which means that value can not be calculated.
   */
  long getWriteSpeedInCacheInMB();

  /**
   * @return Write speed of data in pages per second to file system
   * or value which is less than 0, which means that value can not be calculated.
   */
  long getWriteSpeedInFileInPages();

  /**
   * @return Write speed of data in megabytes per second to file system
   * or value which is less than 0, which means that value can not be calculated.
   */
  long getWriteSpeedInFileInMB();

  /**
   * @return Average time of commit of atomic operation in nanoseconds
   * or value which is less than 0, which means that value can not be calculated.
   */
  long getCommitTimeAvg();

  /**
   * @return Percent of cache hits
   * or value which is less than 0, which means that value can not be calculated.
   */
  int getCacheHits();
}
