package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.parser.OMatchPathItem;
import com.orientechnologies.orient.core.sql.parser.ORid;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
  protected Iterable<OResultInternal> traversePatternEdge(
      OIdentifiable startingPoint, OCommandContext iCommandContext) {

    Object qR = this.item.getMethod().executeReverse(startingPoint, iCommandContext);
    if (qR == null) {
      return Collections.emptyList();
    }
    if (qR instanceof OResultInternal) {
      return Collections.singleton((OResultInternal) qR);
    }
    if (qR instanceof OIdentifiable) {
      return Collections.singleton(new OResultInternal((OIdentifiable) qR));
    }
    if (qR instanceof Iterable) {
      Iterable iterable = (Iterable) qR;
      List<OResultInternal> result = new ArrayList<>();
      for (Object o : iterable) {
        if (o instanceof OIdentifiable) {
          result.add(new OResultInternal((OIdentifiable) o));
        } else if (o instanceof OResultInternal) {
          result.add((OResultInternal) o);
        } else if (o == null) {
          continue;
        } else {
          throw new UnsupportedOperationException();
        }
      }
      return result;
    }
    return Collections.EMPTY_LIST;
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
