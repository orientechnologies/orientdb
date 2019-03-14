package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;

/**
 * Created by luigidellaquila on 22/07/16.
 */
public class SubQueryStep extends AbstractExecutionStep {
  private final OInternalExecutionPlan subExecuitonPlan;
  private final OCommandContext        childCtx;
  private       boolean                sameContextAsParent = false;

  /**
   * executes a sub-query
   *
   * @param subExecutionPlan the execution plan of the sub-query
   * @param ctx              the context of the current execution plan
   * @param subCtx           the context of the subquery execution plan
   */
  public SubQueryStep(OInternalExecutionPlan subExecutionPlan, OCommandContext ctx, OCommandContext subCtx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.subExecuitonPlan = subExecutionPlan;
    this.childCtx = subCtx;

    this.sameContextAsParent = (ctx == childCtx);
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    return subExecuitonPlan.fetchNext(nRecords);
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
