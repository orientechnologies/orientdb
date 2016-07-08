package com.orientechnologies.orient.core.sql.executor;

import java.io.Serializable;

/**
 * Created by luigidellaquila on 06/07/16.
 */
public interface OExecutionPlan extends Serializable{

  public void close();

  public OTodoResultSet fetchNext(int n);

  public String prettyPrint(int indent);
}
