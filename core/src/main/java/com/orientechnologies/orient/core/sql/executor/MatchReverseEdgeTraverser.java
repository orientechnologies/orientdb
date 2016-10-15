package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;

import java.util.Collections;

/**
 * Created by luigidellaquila on 15/10/16.
 */
public class MatchReverseEdgeTraverser extends MatchEdgeTraverser {

  private final String startingPointAlias;
  private final String endPointAlias;

  public MatchReverseEdgeTraverser(OResult lastUpstreamRecord, EdgeTraversal edge) {
    super(lastUpstreamRecord, edge);
    this.startingPointAlias = edge.edge.in.alias;
    this.endPointAlias = edge.edge.out.alias;
  }

  @Override protected Iterable<OIdentifiable> traversePatternEdge(OIdentifiable startingPoint, OCommandContext iCommandContext) {

    Object qR = this.item.getMethod().executeReverse(startingPoint, iCommandContext);
    return (qR instanceof Iterable) ? (Iterable) qR : Collections.singleton((OIdentifiable) qR);
  }

  @Override protected String getStartingPointAlias() {
    return this.startingPointAlias;
  }

  @Override protected String getEndpointAlias() {
    return endPointAlias;
  }

  protected boolean matchesFilters(OCommandContext iCommandContext, OWhereClause filter, OIdentifiable origin) {
    return true;
  }

}
