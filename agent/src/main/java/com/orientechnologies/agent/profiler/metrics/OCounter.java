package com.orientechnologies.agent.profiler.metrics;

/** Created by Enrico Risa on 09/07/2018. */
public interface OCounter extends OMetric {

  default void inc() {}

  default void inc(long n) {}

  default void dec() {}

  default void dec(long n) {}

  default long getCount() {
    return 0;
  }
}
