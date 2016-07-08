package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;

/**
 * Created by luigidellaquila on 06/07/16.
 */
public interface OExecutionStep extends OExecutionCallback {

  OTodoResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException;

  void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException;

  void sendTimeout();

  void setPrevious(OExecutionStep step);

  void setNext(OExecutionStep step);

  void close();

  static String getIndent(int depth, int indent) {
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < depth; i++) {
      for (int j = 0; j < indent; j++) {
        result.append(" ");
      }
    }
    return result.toString();
  }

  default String prettyPrint(int depth, int indent) {
    String spaces = getIndent(depth, indent);
    return spaces + getClass().getSimpleName();
  }
}
