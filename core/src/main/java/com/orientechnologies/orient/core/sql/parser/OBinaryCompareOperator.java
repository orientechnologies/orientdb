package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.sql.executor.metadata.OIndexFinder;

/** Created by luigidellaquila on 12/11/14. */
public interface OBinaryCompareOperator {
  public boolean execute(Object left, Object right);

  boolean supportsBasicCalculation();

  void toGenericStatement(StringBuilder builder);

  OBinaryCompareOperator copy();

  default boolean isRangeOperator() {
    return false;
  }

  public OIndexFinder.Operation getOperation();
}
