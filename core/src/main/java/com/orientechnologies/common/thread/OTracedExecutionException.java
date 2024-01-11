package com.orientechnologies.common.thread;

import com.orientechnologies.common.exception.OException;

public class OTracedExecutionException extends OException {

  public OTracedExecutionException(String message, Exception cause) {
    super(message);
    initCause(cause);
  }

  public OTracedExecutionException(String message) {
    super(message);
  }

  private static String taskName(Object task) {
    if (task != null) {
      return task.getClass().getSimpleName();
    }
    return "?";
  }

  public static OTracedExecutionException prepareTrace(Object task) {
    final OTracedExecutionException trace;
    trace = new OTracedExecutionException(String.format("Async task [%s] failed", taskName(task)));
    trace.fillInStackTrace();
    return trace;
  }

  public static OTracedExecutionException trace(
      OTracedExecutionException trace, Exception e, Object task) throws OTracedExecutionException {
    if (trace != null) {
      trace.initCause(e);
      return trace;
    } else {
      return new OTracedExecutionException(
          String.format("Async task [%s] failed", taskName(task)), e);
    }
  }
}
