package com.orientechnologies.orient.core.sql.parser;

/**
 * Created by luigidellaquila on 12/11/14.
 */
public interface OBinaryCompareOperator {
  public boolean execute(Object left, Object right);

  boolean supportsBasicCalculation();
}
