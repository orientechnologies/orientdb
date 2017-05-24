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
      return leftIdentifier.getStringValue();
    }
    return null;
  }

  public boolean needsAliases(Set<String> aliases) {
    if (aliases.contains(leftIdentifier.getStringValue())) {
      return true;
    }
    if (right.needsAliases(aliases)) {
      return true;
    }
    return false;
  }

  public boolean isAggregate() {
    return right.isAggregate();
  }

  public OJsonItem splitForAggregation(AggregateProjectionSplit aggregateSplit) {
    if (isAggregate()) {
      OJsonItem item = new OJsonItem();
      item.leftIdentifier = leftIdentifier;
      item.leftString = leftString;
      item.right = right.splitForAggregation(aggregateSplit);
      return item;
    } else {
      return this;
    }
  }

  public OJsonItem copy() {
    OJsonItem result = new OJsonItem();
    result.leftIdentifier = leftIdentifier == null ? null : leftIdentifier.copy();
    result.leftString = leftString;
    result.right = right.copy();
    return result;
  }

  public void extractSubQueries(SubQueryCollector collector) {
    right.extractSubQueries(collector);
  }

  public boolean refersToParent() {
    return right != null && right.refersToParent();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OJsonItem oJsonItem = (OJsonItem) o;

    if (leftIdentifier != null ? !leftIdentifier.equals(oJsonItem.leftIdentifier) : oJsonItem.leftIdentifier != null)
      return false;
    if (leftString != null ? !leftString.equals(oJsonItem.leftString) : oJsonItem.leftString != null)
      return false;
    return right != null ? right.equals(oJsonItem.right) : oJsonItem.right == null;
  }

  @Override
  public int hashCode() {
    int result = leftIdentifier != null ? leftIdentifier.hashCode() : 0;
    result = 31 * result + (leftString != null ? leftString.hashCode() : 0);
    result = 31 * result + (right != null ? right.hashCode() : 0);
    return result;
  }
}
