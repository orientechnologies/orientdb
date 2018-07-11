package com.orientechnologies.common.profiler;

import com.orientechnologies.common.profiler.metrics.*;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Created by Enrico Risa on 09/07/2018.
 */
public interface OrientDBProfiler {

  String name(String name, String... names);

  String name(Class<?> klass, String... names);

  OCounter counter(String name, String description);

  OMeter meter(String name, String description);

  <T> OGauge<T> gauge(String name, String description, Supplier<T> valueFunction);

  OHistogram histogram(String name, String description);

  OTimer timer(String name, String description);

  Map<String, OMetric> getMetrics();
}
