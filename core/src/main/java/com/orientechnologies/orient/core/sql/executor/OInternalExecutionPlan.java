package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;

/** Created by luigidellaquila on 06/07/16. */
public interface OInternalExecutionPlan extends OExecutionPlan {

  public static final String JAVA_TYPE = "javaType";

  void close();

  /**
   * if the execution can still return N elements, then the result will contain them all. If the
   * execution contains less than N elements, then the result will contain them all, next result(s)
   * will contain zero elements
   *
   * @param n
   * @return
   */
  OResultSet fetchNext(int n);

  void reset(OCommandContext ctx);

  long getCost();

  default OResult serialize() {
    throw new UnsupportedOperationException();
  }

  default void deserialize(OResult serializedExecutionPlan) {
    throw new UnsupportedOperationException();
  }

  default OInternalExecutionPlan copy(OCommandContext ctx) {
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
