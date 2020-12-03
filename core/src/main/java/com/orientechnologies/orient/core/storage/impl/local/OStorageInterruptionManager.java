package com.orientechnologies.orient.core.storage.impl.local;

import com.orientechnologies.common.concur.lock.OInterruptedException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;

public class OStorageInterruptionManager {

  public void interrupt(Thread thread) {
    synchronized (this) {
      if (getDepth() > 0) {
        setInterrupted(true);
      } else if (thread != null) {
        thread.interrupt();
      }
    }
  }

  public void enterCriticalPath() {
    synchronized (this) {
      if (Thread.currentThread().isInterrupted() || (isInterrupted() && getDepth() == 0)) {
        final Thread thread = Thread.currentThread();
        //        thread.interrupt();
        OLogManager.instance().warnNoDb(this, "Execution  of thread '%s' is interrupted", thread);
        throw new OInterruptedException("Command interrupted");
      }
      incrementDepth();
    }
  }

  public void exitCriticalPath() {
    synchronized (this) {
      decrementDepth();
      if (isInterrupted() && getDepth() == 0) {
        final Thread thread = Thread.currentThread();
        //        thread.interrupt();
        OLogManager.instance().warnNoDb(this, "Execution  of thread '%s' is interrupted", thread);
        throw new OInterruptedException("Command interrupted");
      }
    }
  }

  public boolean isInterrupted() {
    ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (db != null) {
      return db.isCommandInterrupted();
    }
    return false;
  }

  public void setInterrupted(boolean interrupted) {
    ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (db != null) {
      db.setCommandInterrupted(interrupted);
    }
  }

  public int incrementDepth() {
    ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (db != null) {
      int current = db.getCommandInterruptionDepth();
      current++;
      db.setCommandInterruptionDepth(current);
      return current;
    }
    return 0;
  }

  public int decrementDepth() {
    ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (db != null) {
      int current = db.getCommandInterruptionDepth();
      current--;
      db.setCommandInterruptionDepth(current);
      return current;
    }
    return 0;
  }

  public int getDepth() {
    ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (db != null) {
      return db.getCommandInterruptionDepth();
    }
    return 0;
  }
}
