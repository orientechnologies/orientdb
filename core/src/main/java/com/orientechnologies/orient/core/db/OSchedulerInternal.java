package com.orientechnologies.orient.core.db;

import java.util.Date;
import java.util.TimerTask;

public interface OSchedulerInternal {

  void schedule(TimerTask task, long delay, long period);

  void scheduleOnce(TimerTask task, long delay);

  default void scheduleFrom(TimerTask task, Date firstTime, long period) {
    schedule(task, Math.max(0, firstTime.getTime() - System.currentTimeMillis()), period);
  }

  /** Execute a task on executor after a delay
   *
   * @param toExecute
   * @param delay
   */
  OCancellableTimer delayExecute(Runnable toExecute, long delay);

  /** Execute a task on executor at specific time or immediately
   *
   * @param toExecute
   * @param at
   */
  default OCancellableTimer delayExecute(Runnable toExecute, Date at) {
    return delayExecute(toExecute, Math.max(0, at.getTime() - System.currentTimeMillis()));
  }

  /** Add a task on the internal executor periodically.
   *
   * @param toExecute
   * @param periodic
   * @return
   */
  OCancellableTimer periodicExecute(Runnable toExecute, long periodic);

  OCancellableTimer scheduleExecuteFrom(Runnable task, Date firstTime, long period);
}
