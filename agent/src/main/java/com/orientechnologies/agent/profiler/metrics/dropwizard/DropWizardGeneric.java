package com.orientechnologies.agent.profiler.metrics.dropwizard;

import com.codahale.metrics.Metric;

/** Created by Enrico Risa on 11/07/2018. */
public class DropWizardGeneric<T extends Metric> extends DropWizardBase {

  protected final T metric;

  public DropWizardGeneric(T metric, String name, String description) {
    this(metric, name, description, "");
  }

  public DropWizardGeneric(T metric, String name, String description, String unitOfMeasure) {
    super(name, description, unitOfMeasure);
    this.metric = metric;
  }

  public T getMetric() {
    return metric;
  }
}
