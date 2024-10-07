package com.orientechnologies.agent.profiler.metrics;

/** Created by Enrico Risa on 11/07/2018. */
public interface OMetric {

  String getName();

  String getDescription();

  default String getUnitOfMeasure() {
    return "";
  }
}
