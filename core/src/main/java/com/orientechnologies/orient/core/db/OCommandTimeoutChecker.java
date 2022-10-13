package com.orientechnologies.orient.core.db;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

public class OCommandTimeoutChecker {
  private final boolean active;
  private final long maxMills;
  private final ConcurrentHashMap<Thread, Long> running = new ConcurrentHashMap<>();
  private final TimerTask timer;

  public OCommandTimeoutChecker(long timeout, OSchedulerInternal scheduler) {
    this.timer =
        new TimerTask() {
          @Override
          public void run() {
            OCommandTimeoutChecker.this.check();
          }
        };
    this.maxMills = timeout;
    if (timeout > 0) {
      scheduler.schedule(timer, timeout / 10, timeout / 10);
      active = true;
    } else {
      active = false;
    }
  }

  protected void check() {
    if (active) {
      long curTime = System.nanoTime() / 1000000;
      Iterator<Entry<Thread, Long>> iter = running.entrySet().iterator();
      while (iter.hasNext()) {
        Entry<Thread, Long> entry = iter.next();
        if (curTime > entry.getValue()) {
          entry.getKey().interrupt();
          iter.remove();
        }
      }
    }
  }

  public void startCommand(Optional<Long> timeout) {
    if (active) {
      long current = System.nanoTime() / 1000000;
      running.put(Thread.currentThread(), current + timeout.orElse(maxMills));
    }
  }

  public void endCommand() {
    if (active) {
      running.remove(Thread.currentThread());
    }
  }

  public void close() {
    this.timer.cancel();
  }
}
