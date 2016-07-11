package com.orientechnologies.orient.core.sql.parser;

import java.util.Map;
import java.util.Set;

/**
 * Created by luigidellaquila on 18/02/15.
 */
public class OJsonItem {
  protected OIdentifier leftIdentifier;
  protected String      leftString;
  protected OExpression right;

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    if (leftIdentifier != null) {
      builder.append("\"");
      leftIdentifier.toString(params, builder);
      builder.append("\"");
    }
    if (leftString != null) {
      builder.append("\"");
      builder.append(OExpression.encode(leftString));
      builder.append("\"");
    }
    builder.append(": ");
    right.toString(params, builder);
  }

  public String getLeftValue() {
    if (leftString != null) {
      return leftString;
    }
    if (leftIdentifier != null) {
      leftIdentifier.getStringValue();
    }
    return null;
  }

  public boolean needsAliases(Set<String> aliases) {
    if(aliases.contains(leftIdentifier.getStringValue())){
      return true;
    }
    if(right.needsAliases(aliases)){
      return true;
    }
    return false;
  }
}
