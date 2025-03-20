package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;

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
      OInternalExecutionPlan subExecutionPlan, OCommandContext ctx, OCommandContext subCtx) {
    super();
    this.subExecuitonPlan = subExecutionPlan;
    this.childCtx = subCtx;

    this.sameContextAsParent = (ctx == childCtx);
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.start(ctx).close(ctx));
    OExecutionStream parentRs = subExecuitonPlan.start(ctx);
    return parentRs.map(this::mapResult);
  }

  private OResult mapResult(OResult result, OCommandContext ctx) {
    ctx.setCurrent(result);
    return result;
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    StringBuilder builder = new StringBuilder();
    String ind = OExecutionStepInternal.getIndent(ctx);
    builder.append(ind);
    builder.append("+ FETCH FROM SUBQUERY \n");
    ctx.incDepth();
    builder.append(subExecuitonPlan.prettyPrint(ctx));
    ctx.decDepth();
    return builder.toString();
  }

  @Override
  public boolean canBeCached() {
    return sameContextAsParent && subExecuitonPlan.canBeCached();
  }

  @Override
  public OExecutionStepInternal copy(OCommandContext ctx) {
    return new SubQueryStep(subExecuitonPlan.copy(ctx), ctx, ctx);
  }
}
