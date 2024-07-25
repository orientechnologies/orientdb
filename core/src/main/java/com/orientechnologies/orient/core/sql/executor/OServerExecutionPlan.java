package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OServerCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;

public interface OServerExecutionPlan extends OExecutionPlan {

  void close();

  /**
   * if the execution can still return N elements, then the result will contain them all. If the
   * execution contains less than N elements, then the result will contain them all, next result(s)
   * will contain zero elements
   * @param ctx TODO
   *
   * @return
   */
  OExecutionStream start(OServerCommandContext ctx);

  void reset(OCommandContext ctx);

  long getCost();

  default OResult serialize() {
    throw new UnsupportedOperationException();
  }

  default void deserialize(OResult serializedExecutionPlan) {
    throw new UnsupportedOperationException();
  }

  default OServerExecutionPlan copy(OCommandContext ctx) {
    throw new UnsupportedOperationException();
  }

  boolean canBeCached();

  default String getStatement() {
    return null;
  }

  default void setStatement(String stm) {}

  default String getGenericStatement() {
    return null;
  }

  default void setGenericStatement(String stm) {}
}
