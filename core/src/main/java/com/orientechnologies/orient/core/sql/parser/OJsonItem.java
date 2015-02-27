package com.orientechnologies.orient.core.sql.parser;

import java.util.Map;

/**
 * Created by luigidellaquila on 18/02/15.
 */
public class OJsonItem {
  protected OIdentifier leftIdentifier;
  protected String      leftString;
  protected OExpression right;

  public void replaceParameters(Map<Object, Object> params) {
    right.replaceParameters(params);
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    if (leftIdentifier != null) {
      result.append("\"");
      result.append(leftIdentifier.toString());
      result.append("\"");
    }
    if (leftString != null) {
      result.append("\"");
      result.append(OExpression.encode(leftString));
      result.append("\"");
    }
    result.append(": ");
    result.append(right.toString());
    return result.toString();
  }
}
