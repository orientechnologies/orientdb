package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OMatchPathItem;
import com.orientechnologies.orient.core.sql.parser.ORid;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;

/** Created by luigidellaquila on 15/10/16. */
public class MatchReverseEdgeTraverser extends MatchEdgeTraverser {

  private final String startingPointAlias;
  private final String endPointAlias;

  public MatchReverseEdgeTraverser(OResult lastUpstreamRecord, EdgeTraversal edge) {
    super(lastUpstreamRecord, edge);
    this.startingPointAlias = edge.edge.in.alias;
    this.endPointAlias = edge.edge.out.alias;
  }

  protected String targetClassName(OMatchPathItem item, OCommandContext iCommandContext) {
    return edge.getLeftClass();
  }

  protected String targetClusterName(OMatchPathItem item, OCommandContext iCommandContext) {
    return edge.getLeftCluster();
  }

  protected ORid targetRid(OMatchPathItem item, OCommandContext iCommandContext) {
    return edge.getLeftRid();
  }

  protected OWhereClause getTargetFilter(OMatchPathItem item) {
    return edge.getLeftFilter();
  }

  @Override
  protected OExecutionStream traversePatternEdge(
      OIdentifiable startingPoint, OCommandContext iCommandContext) {

    Object qR = this.item.getMethod().executeReverse(startingPoint, iCommandContext);
    if (qR == null) {
      return OExecutionStream.empty();
    }
    if (qR instanceof OResultInternal) {
      return OExecutionStream.singleton((OResultInternal) qR);
    }
    if (qR instanceof OIdentifiable) {
      return OExecutionStream.singleton(new OResultInternal((OIdentifiable) qR));
    }
    if (qR instanceof Iterable) {
      Iterable iterable = (Iterable) qR;
      return OExecutionStream.iterator(iterable.iterator());
    }
    return OExecutionStream.empty();
  }

  @Override
  protected String getStartingPointAlias() {
    return this.startingPointAlias;
  }

  @Override
  protected String getEndpointAlias() {
    return endPointAlias;
  }
}
