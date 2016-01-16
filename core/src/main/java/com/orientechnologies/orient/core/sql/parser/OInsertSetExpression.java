package com.orientechnologies.orient.core.sql.parser;

import java.util.Map;

/**
 * Created by luigidellaquila on 19/02/15.
 */
public class OInsertSetExpression {

  protected OIdentifier left;
  protected OExpression right;

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    left.toString(params, builder);
    builder.append(" = ");
    right.toString(params, builder);

  }
}
