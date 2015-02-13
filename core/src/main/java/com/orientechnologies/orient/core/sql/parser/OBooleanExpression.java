package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.db.record.OIdentifiable;

import java.util.Map;

/**
 * Created by luigidellaquila on 07/11/14.
 */
public abstract class OBooleanExpression extends SimpleNode {

  public static OBooleanExpression TRUE = new OBooleanExpression(0) {
    @Override
    public boolean evaluate(OIdentifiable currentRecord) {
      return true;
    }

    @Override public void replaceParameters(Map<Object, Object> params) {

    }

    @Override
    public String toString() {
      return "true";
    }
  };

  public static OBooleanExpression FALSE = new OBooleanExpression(0) {
    @Override
    public boolean evaluate(OIdentifiable currentRecord) {
      return false;
    }

    @Override public void replaceParameters(Map<Object, Object> params) {

    }

    @Override
    public String toString() {
      return "false";
    }

  };

  public OBooleanExpression(int id) {
    super(id);
  }

  public OBooleanExpression(OrientSql p, int id) {
    super(p, id);
  }

  /** Accept the visitor. **/
  public Object jjtAccept(OrientSqlVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }

  public abstract boolean evaluate(OIdentifiable currentRecord);

  public abstract void replaceParameters(Map<Object, Object> params);
}