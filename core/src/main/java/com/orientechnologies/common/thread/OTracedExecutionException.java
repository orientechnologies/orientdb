package com.orientechnologies.common.thread;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;

public class OTracedExecutionException extends OException {

  public OTracedExecutionException(String message, Exception cause) {
    super(message);
    initCause(cause);
  }

  public OTracedExecutionException(String message) {
    super(message);
  }

  private static String taskName(String taskName, Object task) {
    if (taskName != null) {
      return taskName;
    }
    if (task != null) {
      return task.getClass().getSimpleName();
    }
    return "?";
  }

  public static OTracedExecutionException prepareTrace(String taskName, Object task) {
    final OTracedExecutionException trace;
    if (OLogManager.instance().isDebugEnabled()) {
      trace =
          new OTracedExecutionException(
              String.format("Async task [%s] failed", taskName(taskName, task)));
      trace.fillInStackTrace();
    } else {
      trace = null;
    }
    return trace;
  }

  public static OTracedExecutionException trace(
      OTracedExecutionException trace, Exception e, String taskName, Object task)
      throws OTracedExecutionException {
    if (trace != null) {
      trace.initCause(e);
      return trace;
    } else {
      return new OTracedExecutionException(
          String.format("Async task [%s] failed", taskName(taskName, task)), e);
    }
  }
}
