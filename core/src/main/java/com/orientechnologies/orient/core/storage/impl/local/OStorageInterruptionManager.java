package com.orientechnologies.orient.core.storage.impl.local;

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.log.OLogManager;

public class OStorageInterruptionManager {

  int depth = 0;
  boolean interrupted = false;

  public void interrupt(Thread thread) {
    synchronized (this) {
      if (depth <= 0) {
        interrupted = true;
      } else if (thread != null) {
        thread.interrupt();
      }
    }
  }

  public void enterCriticalPath() {
    synchronized (this) {
      if (Thread.currentThread().isInterrupted() || (interrupted && depth == 0)) {
        final Thread thread = Thread.currentThread();
        thread.interrupt();
        OLogManager.instance().warnNoDb(this, "Execution  of thread '%s' is interrupted", thread);
        throw new OInterruptedException("Command interrupted");
      }
      this.depth++;
    }
  }

  public void exitCriticalPath() {
    synchronized (this) {
      this.depth--;
      if (interrupted && depth == 0) {
        final Thread thread = Thread.currentThread();
        thread.interrupt();
        OLogManager.instance().warnNoDb(this, "Execution  of thread '%s' is interrupted", thread);
        throw new OInterruptedException("Command interrupted");
      }
    }
  }
}
