package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;

/** Created by luigidellaquila on 22/07/16. */
public class SubQueryStep extends AbstractExecutionStep {
  private final OInternalExecutionPlan subExecuitonPlan;
  private final OCommandContext childCtx;
  private boolean sameContextAsParent = false;

  /**
   * executes a sub-query
   *
   * @param subExecutionPlan the execution plan of the sub-query
   * @param ctx the context of the current execution plan
   * @param subCtx the context of the subquery execution plan
   */
  public SubQueryStep(
      OInternalExecutionPlan subExecutionPlan,
      OCommandContext ctx,
      OCommandContext subCtx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.subExecuitonPlan = subExecutionPlan;
    this.childCtx = subCtx;

    this.sameContextAsParent = (ctx == childCtx);
  }

  @Override
  public OExecutionStream syncPull(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx));
    OExecutionStream parentRs = subExecuitonPlan.start();
    return parentRs.map(this::mapResult);
  }

  private OResult mapResult(OResult result, OCommandContext ctx) {
    ctx.setVariable("$current", result);
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    StringBuilder builder = new StringBuilder();
    String ind = OExecutionStepInternal.getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ FETCH FROM SUBQUERY \n");
    builder.append(subExecuitonPlan.prettyPrint(depth + 1, indent));
    return builder.toString();
  }

  @Override
  public boolean canBeCached() {
    return sameContextAsParent && subExecuitonPlan.canBeCached();
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new SubQueryStep(subExecuitonPlan.copy(ctx), ctx, ctx, profilingEnabled);
  }
}
