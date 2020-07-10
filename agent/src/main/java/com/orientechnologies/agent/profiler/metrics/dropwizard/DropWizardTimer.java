package com.orientechnologies.agent.profiler.metrics.dropwizard;

import com.codahale.metrics.Timer;
import com.orientechnologies.agent.profiler.metrics.OSnapshot;
import com.orientechnologies.agent.profiler.metrics.OTimer;

/** Created by Enrico Risa on 11/07/2018. */
public class DropWizardTimer extends DropWizardGeneric<Timer> implements OTimer {

  public DropWizardTimer(Timer timer, String name, String description) {
    super(timer, name, description);
  }

  public long getCount() {
    return metric.getCount();
  }

  @Override
  public OSnapshot getSnapshot() {
    metric.time();
    return new DropWizardSnapshot(metric.getSnapshot());
  }

  @Override
  public OContext time() {
    return new DropWizardTimerContext(metric.time());
  }

  class DropWizardTimerContext implements OTimer.OContext {
    private Timer.Context context;

    public DropWizardTimerContext(Timer.Context context) {
      this.context = context;
    }

    @Override
    public long stop() {
      return context.stop();
    }
  }
}
