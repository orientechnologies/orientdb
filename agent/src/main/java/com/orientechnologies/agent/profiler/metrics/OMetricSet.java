package com.orientechnologies.agent.profiler.metrics;

import java.util.Map;

/** Created by Enrico Risa on 11/07/2018. */
public interface OMetricSet extends OMetric {

  Map<String, OMetric> getMetrics();

  String prefix();
}
