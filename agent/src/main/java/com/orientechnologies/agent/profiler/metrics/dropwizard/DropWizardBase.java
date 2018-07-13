package com.orientechnologies.agent.profiler.metrics.dropwizard;

import com.orientechnologies.common.profiler.metrics.OMetric;

/**
 * Created by Enrico Risa on 11/07/2018.
 */
public class DropWizardBase implements OMetric {
  String name;
  String description;

  public DropWizardBase(String name, String description) {
    this.name = name;
    this.description = description;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }
}
