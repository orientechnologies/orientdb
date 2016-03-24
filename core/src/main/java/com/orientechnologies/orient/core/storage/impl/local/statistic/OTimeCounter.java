package com.orientechnologies.orient.core.storage.impl.local.statistic;

/**
 * Immutable holder for the pair summary time of measurement/count of measurements.
 * This holder is used to gather average values of storage performance parameters in {@link OStoragePerformanceStatistic} class.
 * In multithreading environment this pair of values will be updated in single atomic operation.
 */
final class OTimeCounter {
  /**
   * Summary time of measurement in nanoseconds
   */
  private final long time;
  /**
   * Count of measurements
   */
  private final long counter;

  /**
   * Creates immutable object instance.
   *
   * @param time    Summary time of measurement
   * @param counter Count of measurements
   */
  public OTimeCounter(long time, long counter) {
    this.time = time;
    this.counter = counter;
  }

  public long getTime() {
    return time;
  }

  public long getCounter() {
    return counter;
  }

  /**
   * @return Average time which was spent to perform single operation or -1 if no operations
   * were performed.
   */

  public long calculateAvgTime() {
    if (counter == 0)
      return -1;

    return time / counter;
  }

  /**
   * Interval of time per which speed should be calculated, if passed value equals 1000 it means that speed of
   * operations per ms will be calculated, or -1 if all operations were performed for 0 time.
   *
   * @param interval Interval of time per which speed should be calculated, or -1 if all operations were performed for 0 time.
   * @return Speed of operations per passed in interval, interval is measured in nanoseconds.
   */
  public long calculateSpeedPerTimeInter(long interval) {
    if (time == 0)
      return -1;

    return (counter * interval) / time;
  }
}
