/* Generated By:JJTree: Do not edit this line. OModifier.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import java.util.Map;

public class OModifier extends SimpleNode {

  boolean                    squareBrackets = false;
  OArrayRangeSelector        arrayRange;
  OOrBlock                   condition;
  OArraySingleValuesSelector arraySingleValues;
  OMethodCall                methodCall;
  OSuffixIdentifier          suffix;

  OModifier                  next;

  public OModifier(int id) {
    super(id);
  }

  public OModifier(OrientSql p, int id) {
    super(p, id);
  }

  /** Accept the visitor. **/
  public Object jjtAccept(OrientSqlVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    if (squareBrackets) {
      result.append('[');

      if (arrayRange != null) {
        result.append(arrayRange.toString());
      } else if (condition != null) {
        result.append(condition.toString());
      } else if (arraySingleValues != null) {
        result.append(arraySingleValues.toString());
      }

      result.append(']');
    } else if (methodCall != null) {
      result.append(methodCall.toString());
    } else if (suffix != null) {
      result.append('.');
      result.append(suffix.toString());
    }
    if (next != null) {
      result.append(next.toString());
    }
    return result.toString();
  }

  public void replaceParameters(Map<Object, Object> params) {
    if (arrayRange != null) {
      arrayRange.replaceParameters(params);
    }
    if (condition != null) {
      condition.replaceParameters(params);
    }
    if (arraySingleValues != null) {
      arraySingleValues.replaceParameters(params);
    }
    if (methodCall != null) {
      methodCall.replaceParameters(params);
    }
    if (next != null) {
      next.replaceParameters(params);
    }
  }
}
/* JavaCC - OriginalChecksum=39c21495d02f9b5007b4a2d6915496e1 (do not edit this line) */
