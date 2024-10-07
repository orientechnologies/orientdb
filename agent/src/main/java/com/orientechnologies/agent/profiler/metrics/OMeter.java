package com.orientechnologies.agent.profiler.metrics;

/** Created by Enrico Risa on 09/07/2018. */
public interface OMeter extends OMetric {

  default void mark() {}

  default void mark(long n) {}

  default long getCount() {
    return 0;
  }
}
