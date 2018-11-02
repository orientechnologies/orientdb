package com.orientechnologies.agent.profiler.metrics.dropwizard;

import com.orientechnologies.agent.profiler.metrics.OMetric;

/**
 * Created by Enrico Risa on 11/07/2018.
 */
public class DropWizardBase implements OMetric {
  private final String unitOfMeasure;
  String name;
  String description;

  public DropWizardBase(String name, String description) {
    this(name, description, "");
  }

  public DropWizardBase(String name, String description, String unitOfMeasure) {
    this.name = name;
    this.description = description;
    this.unitOfMeasure = unitOfMeasure;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  @Override
  public String getUnitOfMeasure() {
    return unitOfMeasure;
  }
}
