package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;

/** Created by luigidellaquila on 17/10/16. */
public class OptionalMatchStep extends MatchStep {
  public OptionalMatchStep(OCommandContext context, EdgeTraversal edge, boolean profilingEnabled) {
    super(context, edge, profilingEnabled);
  }

  @Override
  protected MatchEdgeTraverser createTraverser(OResult lastUpstreamRecord) {
    return new OptionalMatchEdgeTraverser(lastUpstreamRecord, edge);
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ OPTIONAL MATCH ");
    if (edge.out) {
      result.append(" ---->\n");
    } else {
      result.append("     <----\n");
    }
    result.append(spaces);
    result.append("  ");
    result.append("{" + edge.edge.out.alias + "}");
    result.append(edge.edge.item.getMethod());
    result.append("{" + edge.edge.in.alias + "}");
    return result.toString();
  }
}
