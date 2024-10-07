package com.orientechnologies.agent.profiler.metrics;

/** Created by Enrico Risa on 09/07/2018. */
public interface OGauge<T> extends OMetric {

  default T getValue() {
    return null;
  }
}
