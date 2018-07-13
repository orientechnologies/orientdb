package com.orientechnologies.agent.profiler.metrics.dropwizard;

import com.codahale.metrics.Gauge;
import com.orientechnologies.common.profiler.metrics.OGauge;

/**
 * Created by Enrico Risa on 11/07/2018.
 */
public class DropWizardGauge<T> extends DropWizardBase implements OGauge<T> {

  private Gauge<T> meter;

  public DropWizardGauge(Gauge<T> gauge, String name, String description) {
    super(name, description);
    this.meter = gauge;

  }

  @Override
  public T getValue() {
    return meter.getValue();
  }
}
