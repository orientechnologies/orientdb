package com.orientechnologies.common.profiler.metrics;

/**
 * Created by Enrico Risa on 09/07/2018.
 */
public interface OMeter extends OMetric {

  void mark();

  void mark(long n);

  long getCount();
}
