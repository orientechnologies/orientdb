package com.orientechnologies.agent.profiler.metrics.dropwizard;

import com.codahale.metrics.Counter;
import com.orientechnologies.agent.profiler.metrics.OCounter;

/** Created by Enrico Risa on 11/07/2018. */
public class DropWizardCounter extends DropWizardGeneric<Counter> implements OCounter {

  public DropWizardCounter(Counter counter, String name, String description) {
    super(counter, name, description);
  }

  public void inc() {
    metric.inc();
  }

  public void inc(long n) {
    metric.inc(n);
  }

  public void dec() {
    metric.dec();
  }

  public void dec(long n) {
    metric.dec(n);
  }

  public long getCount() {
    return metric.getCount();
  }
}
