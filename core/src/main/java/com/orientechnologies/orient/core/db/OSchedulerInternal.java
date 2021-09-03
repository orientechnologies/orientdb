package com.orientechnologies.orient.core.db;

import java.util.TimerTask;

public interface OSchedulerInternal {

  void schedule(TimerTask task, long delay, long period);

  void scheduleOnce(TimerTask task, long delay);
}
