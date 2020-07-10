package com.orientechnologies.agent.services.metrics;

import java.util.ArrayList;
import java.util.List;

/** Created by Enrico Risa on 18/07/2018. */
public class OrientDBMetricSupport implements OrientDBMetric {

  List<OrientDBMetric> metrics = new ArrayList<>();

  @Override
  public void start() {
    metrics.forEach(m -> m.start());
  }

  @Override
  public void stop() {
    metrics.forEach(m -> m.stop());
    metrics.clear();
  }

  public boolean add(OrientDBMetric orientDBMetric) {
    return metrics.add(orientDBMetric);
  }
}
