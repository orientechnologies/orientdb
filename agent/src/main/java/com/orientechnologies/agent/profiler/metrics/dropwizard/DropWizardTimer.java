package com.orientechnologies.agent.profiler.metrics.dropwizard;

import com.codahale.metrics.Timer;
import com.orientechnologies.common.profiler.metrics.OSnapshot;
import com.orientechnologies.common.profiler.metrics.OTimer;

/**
 * Created by Enrico Risa on 11/07/2018.
 */
public class DropWizardTimer extends DropWizardBase implements OTimer {

  private Timer timer;

  public DropWizardTimer(Timer timer, String name, String description) {
    super(name, description);
    this.timer = timer;

  }

  public long getCount() {
    return timer.getCount();
  }

  @Override
  public OSnapshot getSnapshot() {
    timer.time();
    return new DropWizardSnapshot(timer.getSnapshot());
  }

  @Override
  public OContext time() {
    return new DropWizardTimerContext(timer.time());
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
