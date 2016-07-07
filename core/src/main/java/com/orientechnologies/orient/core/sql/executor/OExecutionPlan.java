package com.orientechnologies.orient.core.sql.executor;

/**
 * Created by luigidellaquila on 06/07/16.
 */
public interface OExecutionPlan {

  public void close();

  public OTodoResultSet fetchNext(int n);
}
