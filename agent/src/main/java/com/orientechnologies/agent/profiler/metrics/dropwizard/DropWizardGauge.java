package com.orientechnologies.agent.profiler.metrics.dropwizard;

import com.codahale.metrics.Gauge;
import com.orientechnologies.agent.profiler.metrics.OGauge;

/**
 * Created by Enrico Risa on 11/07/2018.
 */
public class DropWizardGauge<T> extends DropWizardGeneric<Gauge<T>> implements OGauge<T> {

  public DropWizardGauge(Gauge<T> gauge, String name, String description) {
    this(gauge,name,description,"");

  }

  public DropWizardGauge(Gauge<T> gauge, String name, String description, String unitOfMeasure) {
    super(gauge,name,description,unitOfMeasure);
  }

  @Override
  public T getValue() {
    return metric.getValue();
  }
}
