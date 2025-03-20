package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import java.util.List;

/** Created by luigidellaquila on 20/09/16. */
public class MatchFirstStep extends AbstractExecutionStep {
  private final PatternNode node;
  private OInternalExecutionPlan executionPlan;

  public MatchFirstStep(OCommandContext context, PatternNode node) {
    this(node, null);
  }

  public MatchFirstStep(PatternNode node, OInternalExecutionPlan subPlan) {
    super();
    this.node = node;
    this.executionPlan = subPlan;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.start(ctx).close(ctx));
    OExecutionStream data;
    String alias = getAlias();
    List<OResult> matchedNodes =
        (List<OResult>) ctx.getVariable(MatchPrefetchStep.PREFETCHED_MATCH_ALIAS_PREFIX + alias);
    if (matchedNodes != null) {
      data = OExecutionStream.resultCollection(matchedNodes);
    } else {
      data = executionPlan.start(ctx);
    }

    return data.map(
        (result, context) -> {
          OResultInternal newResult = new OResultInternal();
          newResult.setProperty(getAlias(), result);
          context.setVariable("$matched", newResult);
          return newResult;
        });
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    String spaces = OExecutionStepInternal.getIndent(ctx);
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
      ctx.incDepth();
      result.append(executionPlan.prettyPrint(ctx));
      ctx.decDepth();
    }

    return result.toString();
  }

  private String getAlias() {
    return this.node.getAlias();
  }
}
