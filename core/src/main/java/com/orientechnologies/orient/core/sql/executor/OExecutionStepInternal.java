package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;

import java.util.Collections;
import java.util.List;

/**
 * Created by luigidellaquila on 06/07/16.
 */
public interface OExecutionStepInternal extends OExecutionStep, OExecutionCallback {

  OTodoResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException;

  void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException;

  void sendTimeout();

  void setPrevious(OExecutionStepInternal step);

  void setNext(OExecutionStepInternal step);

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

  default String getName() {
    return getClass().getSimpleName();
  }

  default String getType() {
    return getClass().getSimpleName();
  }

  default String getDescription() {
    return prettyPrint(0, 3);
  }

  default String getTargetNode() {
    return "<local>";
  }

  default List<OExecutionStep> getSubSteps() {
    return Collections.EMPTY_LIST;
  }

  default List<OExecutionPlan> getSubExecutionPlans() {
    return Collections.EMPTY_LIST;
  }

  default void reset() {
    //do nothing
  }

}
