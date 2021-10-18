package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import java.util.Map;
import java.util.Optional;

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
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    OResultSet parentRs = subExecuitonPlan.fetchNext(nRecords);
    return new OResultSet() {
      @Override
      public boolean hasNext() {
        return parentRs.hasNext();
      }

      @Override
      public OResult next() {
        OResult item = parentRs.next();
        ctx.setVariable("$current", item);
        return item;
      }

      @Override
      public void close() {
        parentRs.close();
      }

      @Override
      public Optional<OExecutionPlan> getExecutionPlan() {
        return Optional.empty();
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };
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
