package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OIteratorResultSet;
import com.orientechnologies.orient.core.sql.executor.resultset.OResultSetMapper;
import com.orientechnologies.orient.core.sql.parser.OLocalResultSet;
import java.util.List;

/** Created by luigidellaquila on 20/09/16. */
public class MatchFirstStep extends AbstractExecutionStep {
  private final PatternNode node;
  private OInternalExecutionPlan executionPlan;

  public MatchFirstStep(OCommandContext context, PatternNode node, boolean profilingEnabled) {
    this(context, node, null, profilingEnabled);
  }

  public MatchFirstStep(
      OCommandContext context,
      PatternNode node,
      OInternalExecutionPlan subPlan,
      boolean profilingEnabled) {
    super(context, profilingEnabled);
    this.node = node;
    this.executionPlan = subPlan;
  }

  @Override
  public void reset() {
    if (executionPlan != null) {
      executionPlan.reset(this.getContext());
    }
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx));
    OResultSet data;
    String alias = getAlias();
    List<OResult> matchedNodes =
        (List<OResult>) ctx.getVariable(MatchPrefetchStep.PREFETCHED_MATCH_ALIAS_PREFIX + alias);
    if (matchedNodes != null) {
      data = new OIteratorResultSet(matchedNodes.iterator());
    } else {
      data = new OLocalResultSet(executionPlan);
    }

    return new OResultSetMapper(
        data,
        (result) -> {
          OResultInternal newResult = new OResultInternal();
          newResult.setProperty(getAlias(), result);
          ctx.setVariable("$matched", newResult);
          return newResult;
        });
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ SET \n");
    result.append(spaces);
    result.append("   ");
    result.append(getAlias());
    if (executionPlan != null) {
      result.append("\n");
      result.append(spaces);
      result.append("  AS\n");
      result.append(executionPlan.prettyPrint(depth + 1, indent));
    }

    return result.toString();
  }

  private String getAlias() {
    return this.node.alias;
  }
}
