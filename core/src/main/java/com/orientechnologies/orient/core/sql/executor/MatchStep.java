package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OEdgeTraverserExcutionStream;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OFieldMatchPathItem;
import com.orientechnologies.orient.core.sql.parser.OMultiMatchPathItem;

/** @author Luigi Dell'Aquila */
public class MatchStep extends AbstractExecutionStep {
  protected final EdgeTraversal edge;

  public MatchStep(EdgeTraversal edge) {
    super();
    this.edge = edge;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    OExecutionStream resultSet = getPrev().get().start(ctx);
    return resultSet.flatMap(this::createNextResultSet);
  }

  public OExecutionStream createNextResultSet(OResult lastUpstreamRecord, OCommandContext ctx) {
    MatchEdgeTraverser trav = createTraverser(lastUpstreamRecord);
    return new OEdgeTraverserExcutionStream(trav);
  }

  protected MatchEdgeTraverser createTraverser(OResult lastUpstreamRecord) {
    if (edge.edge.getItem() instanceof OMultiMatchPathItem) {
      return new MatchMultiEdgeTraverser(lastUpstreamRecord, edge);
    } else if (edge.edge.getItem() instanceof OFieldMatchPathItem) {
      return new MatchFieldTraverser(lastUpstreamRecord, edge);
    } else if (edge.out) {
      return new MatchEdgeTraverser(lastUpstreamRecord, edge);
    } else {
      return new MatchReverseEdgeTraverser(lastUpstreamRecord, edge);
    }
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    String spaces = OExecutionStepInternal.getIndent(ctx);
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
    result.append("{" + edge.edge.getOut().getAlias() + "}");
    if (edge.edge.getItem() instanceof OFieldMatchPathItem) {
      result.append(".");
      result.append(((OFieldMatchPathItem) edge.edge.getItem()).getField());
    } else {
      result.append(edge.edge.getItem().getMethod());
    }
    result.append("{" + edge.edge.getIn().getAlias() + "}");
    return result.toString();
  }
}
