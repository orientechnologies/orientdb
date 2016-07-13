package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;

/**
 * Created by luigidellaquila on 06/07/16.
 */
public interface OInternalExecutionPlan extends OExecutionPlan{
  public void close();

  public OTodoResultSet fetchNext(int n);

  public void reset(OCommandContext ctx);
}
