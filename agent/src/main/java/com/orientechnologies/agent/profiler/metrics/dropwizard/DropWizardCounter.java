package com.orientechnologies.agent.profiler.metrics.dropwizard;

import com.codahale.metrics.Counter;
import com.orientechnologies.agent.profiler.metrics.OCounter;

/**
 * Created by Enrico Risa on 11/07/2018.
 */
public class DropWizardCounter extends DropWizardBase implements OCounter {

  private Counter counter;

  public DropWizardCounter(Counter counter, String name, String description) {

    super(name, description);
    this.counter = counter;

  }

  public void inc() {
    counter.inc();
  }

  public void inc(long n) {
    counter.inc(n);
  }

  public void dec() {
    counter.dec();
  }

  public void dec(long n) {
    counter.dec(n);
  }

  public long getCount() {
    return counter.getCount();
  }
}
