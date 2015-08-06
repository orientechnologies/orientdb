package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;

import java.util.Collections;

/**
 * @author Luigi Dell'Aquila
 */
public class OMatchPathItemFirst extends OMatchPathItem {
  protected OFunctionCall function;

  public OMatchPathItemFirst(int id) {
    super(id);
  }

  public OMatchPathItemFirst(OrientSql p, int id) {
    super(p, id);
  }

  public boolean isBidirectional() {
    return false;
  }

  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(function.toString());
    if (filter != null) {
      result.append(filter.toString());
    }
    return result.toString();
  }

  protected Iterable<OIdentifiable> traversePatternEdge(OMatchStatement.MatchContext matchContext, OIdentifiable startingPoint,
      OCommandContext iCommandContext) {
    Object qR = this.function.execute(startingPoint, iCommandContext);
    return (qR instanceof Iterable) ? (Iterable) qR : Collections.singleton(qR);
  }
}
