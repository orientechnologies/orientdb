package com.orientechnologies.agent.profiler.metrics.dropwizard;

import com.codahale.metrics.Snapshot;
import com.orientechnologies.agent.profiler.metrics.OSnapshot;

/** Created by Enrico Risa on 11/07/2018. */
public class DropWizardSnapshot implements OSnapshot {

  private Snapshot snapshot;

  public DropWizardSnapshot(Snapshot snapshot) {
    this.snapshot = snapshot;
  }

  public int size() {
    return snapshot.size();
  }

  public double getMedian() {
    return snapshot.getMedian();
  }

  public long getMax() {
    return snapshot.getMax();
  }

  public double getMean() {
    return snapshot.getMean();
  }

  public long getMin() {
    return snapshot.getMin();
  }
}
