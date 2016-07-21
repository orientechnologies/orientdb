package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;

/**
 * Created by luigidellaquila on 06/07/16.
 */
public interface OInternalExecutionPlan extends OExecutionPlan{
  void close();

  /**
   * if the execution can still return N elements, then the result will contain them all.
   * If the execution contains less than N elements, then the result will contain them all, next result(s) will contain zero elements
   * @param n
   * @return
   */
  OTodoResultSet fetchNext(int n);

  void reset(OCommandContext ctx);
}
