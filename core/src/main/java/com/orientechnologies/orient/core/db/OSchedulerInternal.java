package com.orientechnologies.orient.core.db;

public interface OSchedulerInternal {

  void schedule(OTimerTask task, long delay, long period);

  void scheduleOnce(OTimerTask task, long delay);
}
