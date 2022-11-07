package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import java.util.Collections;
import java.util.Map;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OMatchPathItemFirst extends OMatchPathItem {
  protected OFunctionCall function;

  protected OMethodCall methodWrapper;

  public OMatchPathItemFirst(int id) {
    super(id);
  }

  public OMatchPathItemFirst(OrientSql p, int id) {
    super(p, id);
  }

  public boolean isBidirectional() {
    return false;
  }

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    function.toString(params, builder);
    if (filter != null) {
      filter.toString(params, builder);
    }
  }

  public void toGenericStatement(StringBuilder builder) {
    function.toGenericStatement(builder);
    if (filter != null) {
      filter.toGenericStatement(builder);
    }
  }

  protected Iterable<OIdentifiable> traversePatternEdge(
      OMatchStatement.MatchContext matchContext,
      OIdentifiable startingPoint,
      OCommandContext iCommandContext) {
    Object qR = this.function.execute(startingPoint, iCommandContext);
    return (qR instanceof Iterable) ? (Iterable) qR : Collections.singleton((OIdentifiable) qR);
  }

  @Override
  public OMatchPathItem copy() {
    OMatchPathItemFirst result = (OMatchPathItemFirst) super.copy();
    result.function = function == null ? null : function.copy();
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }
    OMatchPathItemFirst that = (OMatchPathItemFirst) o;

    if (function != null ? !function.equals(that.function) : that.function != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (function != null ? function.hashCode() : 0);
    return result;
  }

  public OFunctionCall getFunction() {
    return function;
  }

  public void setFunction(OFunctionCall function) {
    this.function = function;
  }

  @Override
  public OMethodCall getMethod() {
    if (methodWrapper == null) {
      synchronized (this) {
        if (methodWrapper == null) {
          methodWrapper = new OMethodCall(-1);
          methodWrapper.params = function.params;
          methodWrapper.methodName = function.name;
        }
      }
    }
    return methodWrapper;
  }
}
