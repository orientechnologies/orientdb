package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OAdminCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;

public interface OAdminExecutionPlan extends OExecutionPlanContextOps {

  void close();

  /**
   * if the execution can still return N elements, then the result will contain them all. If the
   * execution contains less than N elements, then the result will contain them all, next result(s)
   * will contain zero elements
   * @param ctx server command context
   *
   * @return
   */
  OExecutionStream start(OAdminCommandContext ctx);

  long getCost();

  default String getStatement() {
    return null;
  }

  default void setStatement(String stm) {}

  default String getGenericStatement() {
    return null;
  }

  default void setGenericStatement(String stm) {}
}
