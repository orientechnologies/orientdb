package com.orientechnologies.common.util;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;

/**
 * Handler which is used to log all exceptions which are left uncaught by any exception handler.
 */
public class OUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
  @Override
  public void uncaughtException(Thread t, Throwable e) {
    final OLogManager logManager = OLogManager.instance();

    if (logManager != null) {
      OLogManager.instance().error(this, "Uncaught exception in thread %s", e, t.getName());
    }

    if (e instanceof Error) {
      final Orient orient = Orient.instance();

      if (orient != null) {
        orient.handleJVMError((Error) e);
      }
    }

  }
}
