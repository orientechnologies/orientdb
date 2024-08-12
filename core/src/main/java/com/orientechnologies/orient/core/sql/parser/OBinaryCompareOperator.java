package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.sql.executor.metadata.OIndexFinder;

/** Created by luigidellaquila on 12/11/14. */
public interface OBinaryCompareOperator {
  public boolean execute(Object left, Object right);

  boolean supportsBasicCalculation();

  void toGenericStatement(StringBuilder builder);

  OBinaryCompareOperator copy();

  default boolean isRange() {
    return false;
  }

  public OIndexFinder.Operation getOperation();

  public boolean isInclude();

  public boolean isLess();

  public boolean isGreater();

  public default boolean isGreaterInclude() {
    return isGreater() && isInclude();
  }

  public default boolean isLessInclude() {
    return isLess() && isInclude();
  }
}
