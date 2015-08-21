package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;

/**
 * Created by luigidellaquila on 28/07/15.
 */
class PatternEdge {
  PatternNode    in;
  PatternNode    out;
  OMatchPathItem item;

  protected Iterable<OIdentifiable> executeTraversal(OMatchStatement.MatchContext matchContext, OCommandContext iCommandContext,
      OIdentifiable startingPoint, int depth) {
    return item.executeTraversal(matchContext, iCommandContext, startingPoint, depth);
  }

}
