package com.orientechnologies.agent.profiler.metrics.dropwizard;

import com.codahale.metrics.Histogram;
import com.orientechnologies.common.profiler.metrics.OHistogram;
import com.orientechnologies.common.profiler.metrics.OSnapshot;

/**
 * Created by Enrico Risa on 11/07/2018.
 */
public class DropWizardHistogram extends DropWizardBase implements OHistogram {

  private Histogram histogram;

  public DropWizardHistogram(Histogram histogram, String name, String description) {
    super(name, description);
    this.histogram = histogram;

  }

  public void update(int value) {
    histogram.update(value);
  }

  public void update(long value) {
    histogram.update(value);
  }

  public long getCount() {
    return histogram.getCount();
  }

  @Override
  public OSnapshot getSnapshot() {
    return new DropWizardSnapshot(histogram.getSnapshot());
  }
}
