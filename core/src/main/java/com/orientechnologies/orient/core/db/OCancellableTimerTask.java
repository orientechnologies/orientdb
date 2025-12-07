package com.orientechnologies.orient.core.db;

import java.util.TimerTask;

public record OCancellableTimerTask(TimerTask task) implements OCancellableTimer {

  @Override
  public void cancel() {
    task.cancel();
  }
}
