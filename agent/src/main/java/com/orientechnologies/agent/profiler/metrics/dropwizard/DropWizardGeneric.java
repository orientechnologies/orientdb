package com.orientechnologies.agent.profiler.metrics.dropwizard;

import com.codahale.metrics.Metric;

/**
 * Created by Enrico Risa on 11/07/2018.
 */
public class DropWizardGeneric extends DropWizardBase {

  private final Metric metric;

  public DropWizardGeneric(Metric metric, String name, String description) {
    super(name, description);
    this.metric = metric;
  }

}
