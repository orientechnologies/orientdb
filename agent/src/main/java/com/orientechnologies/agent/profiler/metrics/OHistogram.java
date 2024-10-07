package com.orientechnologies.agent.profiler.metrics;

/** Created by Enrico Risa on 09/07/2018. */
public interface OHistogram extends OMetric {

  default void update(int value) {}

  default void update(long value) {}

  default long getCount() {
    return 0;
  }

  default OSnapshot getSnapshot() {
    return null;
  }
}
