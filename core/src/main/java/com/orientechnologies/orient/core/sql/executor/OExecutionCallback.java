package com.orientechnologies.orient.core.sql.executor;

/**
 * Created by luigidellaquila on 06/07/16.
 */
public interface OExecutionCallback {

  public static enum Status {
    DATA_AVAILABLE_IN_REQUEST, LAST_RESULT_IN_REQUEST, LAST_RESULT, NO_MORE_RESULT;
  }

  void sendResult(Object o, Status status);

}
