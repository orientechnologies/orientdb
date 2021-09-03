package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.parser.OMatchPathItem;
import com.orientechnologies.orient.core.sql.parser.OMatchStatement;

/** Created by luigidellaquila on 28/07/15. */
public class PatternEdge {
  public PatternNode in;
  public PatternNode out;
  public OMatchPathItem item;

  public Iterable<OIdentifiable> executeTraversal(
      OMatchStatement.MatchContext matchContext,
      OCommandContext iCommandContext,
      OIdentifiable startingPoint,
      int depth) {
    return item.executeTraversal(matchContext, iCommandContext, startingPoint, depth);
  }

  @Override
  public String toString() {
    return "{as: " + in.alias + "}" + item.toString();
  }
}
