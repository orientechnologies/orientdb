package com.orientechnologies.orient.distributed.context.coordination.dbs;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class OWatcher {

  private volatile List<ONotificationActionData> notifications;

  protected interface WaitCond {
    /*
     * Return false to wait true to action
     */
    boolean match();
  }

  protected record ONotificationActionData(WaitCond cond, ONotificationAction action) {}

  /** Wait for a condition, it returns true if the condition is matched false
   *  in case of timeout.
   * @param timeout
   * @param cond
   * @return
   * @throws InterruptedException
   */
  protected synchronized boolean waitFor(Optional<Long> timeout, WaitCond cond)
      throws InterruptedException {
    if (timeout.isPresent()) {
      var timeOut = timeout.get();
      long start = currentTime();
      long till = start + timeOut;
      while (!cond.match() && timeOut > 0) {
        this.wait(timeOut);
        long current = currentTime();
        timeOut = till - current;
      }
      return timeOut > 0;
    } else {
      while (!cond.match()) {
        this.wait();
      }
      return true;
    }
  }

  private long currentTime() {
    return System.nanoTime() / 1000;
  }

  protected synchronized void executeOn(WaitCond cond, ONotificationAction execute) {
    if (notifications == null) {
      notifications = new ArrayList<>();
    }
    if (cond.match()) {
      execute.execute();
    } else {
      this.notifications.add(new ONotificationActionData(cond, execute));
    }
  }

  protected synchronized void notifyChange() {
    if (this.notifications != null) {
      Iterator<ONotificationActionData> iter = this.notifications.iterator();
      while (iter.hasNext()) {
        ONotificationActionData act = iter.next();
        if (act.cond().match()) {
          act.action().execute();
          iter.remove();
        }
      }
    }
    this.notifyAll();
  }
}
