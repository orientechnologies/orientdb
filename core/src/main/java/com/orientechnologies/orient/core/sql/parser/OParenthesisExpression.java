/* Generated By:JJTree: Do not edit this line. OParenthesisExpression.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import java.util.Map;

public class OParenthesisExpression extends OMathExpression {

  protected OExpression expression;
  protected OStatement  statement;

  public OParenthesisExpression(int id) {
    super(id);
  }

  public OParenthesisExpression(OrientSql p, int id) {
    super(p, id);
  }

  /** Accept the visitor. **/
  public Object jjtAccept(OrientSqlVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append('(');
    if (expression != null) {
      result.append(expression.toString());
    } else if (statement != null) {
      result.append(statement.toString());
    }
    result.append(')');
    return result.toString();
  }

  @Override
  public void replaceParameters(Map<Object, Object> params) {
    super.replaceParameters(params);
    if (expression != null) {
      expression.replaceParameters(params);
    }
    if (statement != null) {
      statement.replaceParameters(params);
    }
  }
}
/* JavaCC - OriginalChecksum=4656e5faf4f54dc3fc45a06d8e375c35 (do not edit this line) */
