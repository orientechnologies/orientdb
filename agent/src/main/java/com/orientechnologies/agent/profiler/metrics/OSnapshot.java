package com.orientechnologies.agent.profiler.metrics;

/** Created by Enrico Risa on 11/07/2018. */
public interface OSnapshot {

  default int size() {
    return 0;
  }

  default double getMedian() {
    return 0;
  }

  default long getMax() {
    return 0;
  }

  default double getMean() {
    return 0;
  }

  default long getMin() {
    return 0;
  }
}
