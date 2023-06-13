package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OResultSetEdgeTraverser;
import com.orientechnologies.orient.core.sql.executor.resultset.OSubResultsResultSet;
import com.orientechnologies.orient.core.sql.parser.OFieldMatchPathItem;
import com.orientechnologies.orient.core.sql.parser.OMultiMatchPathItem;

/** @author Luigi Dell'Aquila */
public class MatchStep extends AbstractExecutionStep {
  protected final EdgeTraversal edge;

  public MatchStep(OCommandContext context, EdgeTraversal edge, boolean profilingEnabled) {
    super(context, profilingEnabled);
    this.edge = edge;
  }

  @Override
  public void reset() {}

  @Override
  public OResultSet syncPull(OCommandContext ctx) throws OTimeoutException {
    OResultSet resultSet = getPrev().get().syncPull(ctx);
    OResultSet resSet =
        new OSubResultsResultSet(
            resultSet.stream()
                .map(
                    (res) -> {
                      return createNextResultSet(res, ctx);
                    })
                .iterator());
    return resSet;
  }

  public OResultSet createNextResultSet(OResult lastUpstreamRecord, OCommandContext ctx) {
    MatchEdgeTraverser trav = createTraverser(lastUpstreamRecord);
    return new OResultSetEdgeTraverser(ctx, trav);
  }

  protected MatchEdgeTraverser createTraverser(OResult lastUpstreamRecord) {
    if (edge.edge.item instanceof OMultiMatchPathItem) {
      return new MatchMultiEdgeTraverser(lastUpstreamRecord, edge);
    } else if (edge.edge.item instanceof OFieldMatchPathItem) {
      return new MatchFieldTraverser(lastUpstreamRecord, edge);
    } else if (edge.out) {
      return new MatchEdgeTraverser(lastUpstreamRecord, edge);
    } else {
      return new MatchReverseEdgeTraverser(lastUpstreamRecord, edge);
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ MATCH ");
    if (edge.out) {
      result.append("     ---->\n");
    } else {
      result.append("     <----\n");
    }
    result.append(spaces);
    result.append("  ");
    result.append("{" + edge.edge.out.alias + "}");
    if (edge.edge.item instanceof OFieldMatchPathItem) {
      result.append(".");
      result.append(((OFieldMatchPathItem) edge.edge.item).getField());
    } else {
      result.append(edge.edge.item.getMethod());
    }
    result.append("{" + edge.edge.in.alias + "}");
    return result.toString();
  }
}
