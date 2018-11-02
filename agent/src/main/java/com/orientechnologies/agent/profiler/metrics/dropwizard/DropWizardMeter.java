package com.orientechnologies.agent.profiler.metrics.dropwizard;

import com.codahale.metrics.Meter;
import com.orientechnologies.agent.profiler.metrics.OMeter;

/**
 * Created by Enrico Risa on 11/07/2018.
 */
public class DropWizardMeter extends DropWizardBase implements OMeter {

  private Meter meter;

  public DropWizardMeter(Meter meter, String name, String description) {
    this(meter,name,description,"");
  }

  public DropWizardMeter(Meter meter, String name, String description,String unitOfMeasure) {
    super(name, description,unitOfMeasure);
    this.meter = meter;
  }

  public void mark() {
    meter.mark();
  }

  public void mark(long n) {
    meter.mark(n);
  }

  public long getCount() {
    return meter.getCount();
  }



}
