package com.orientechnologies.common.util;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;

/** Handler which is used to log all exceptions which are left uncaught by any exception handler. */
public class OUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
  private static final OLogger logger =
      OLogManager.instance().logger(OUncaughtExceptionHandler.class);

  @Override
  public void uncaughtException(Thread t, Throwable e) {
    logger.errorNoDb("Uncaught exception in thread %s", e, t.getName());
  }
}
