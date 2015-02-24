package com.orientechnologies.orient.core.sql.parser;

import java.util.Map;

/**
 * Created by luigidellaquila on 19/02/15.
 */
public class OInsertSetExpression {

  protected OIdentifier left;
  protected OExpression right;

  public void replaceParameters(Map<Object, Object> params) {
    right.replaceParameters(params);
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(left.toString());
    result.append(" = ");
    result.append(right.toString());
    return result.toString();
  }
}
