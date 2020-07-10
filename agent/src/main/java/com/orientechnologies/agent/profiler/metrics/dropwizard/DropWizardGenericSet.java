package com.orientechnologies.agent.profiler.metrics.dropwizard;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.orientechnologies.agent.profiler.metrics.OMetric;
import com.orientechnologies.agent.profiler.metrics.OMetricSet;
import java.util.Map;
import java.util.stream.Collectors;

/** Created by Enrico Risa on 11/07/2018. */
public class DropWizardGenericSet extends DropWizardGeneric<MetricSet> implements OMetricSet {

  public DropWizardGenericSet(MetricSet metric, String name, String description) {
    super(metric, name, description);
  }

  @Override
  public Map<String, OMetric> getMetrics() {

    return metric.getMetrics().entrySet().stream()
        .collect(
            Collectors.toMap(
                (entry) -> entry.getKey(),
                (entry) -> {
                  Metric m = entry.getValue();
                  if (m instanceof MetricSet) {
                    return new DropWizardGenericSet((MetricSet) m, entry.getKey(), "");
                  }
                  return new DropWizardGeneric(m, entry.getKey(), "");
                }));
  }

  @Override
  public String prefix() {
    return name;
  }
}
