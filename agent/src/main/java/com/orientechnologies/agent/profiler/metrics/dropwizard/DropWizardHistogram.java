package com.orientechnologies.agent.profiler.metrics.dropwizard;

import com.codahale.metrics.Histogram;
import com.orientechnologies.agent.profiler.metrics.OHistogram;
import com.orientechnologies.agent.profiler.metrics.OSnapshot;

/** Created by Enrico Risa on 11/07/2018. */
public class DropWizardHistogram extends DropWizardGeneric<Histogram> implements OHistogram {

  public DropWizardHistogram(Histogram histogram, String name, String description) {
    super(histogram, name, description);
  }

  public void update(int value) {
    metric.update(value);
  }

  public void update(long value) {
    metric.update(value);
  }

  public long getCount() {
    return metric.getCount();
  }

  @Override
  public OSnapshot getSnapshot() {
    return new DropWizardSnapshot(metric.getSnapshot());
  }
}
