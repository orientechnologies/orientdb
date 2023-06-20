package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OProjection;

/** Created by luigidellaquila on 12/07/16. */
public class ProjectionCalculationStep extends AbstractExecutionStep {
  protected final OProjection projection;

  protected long cost = 0;

  public ProjectionCalculationStep(
      OProjection projection, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.projection = projection;
  }

  @Override
  public OExecutionStream syncPull(OCommandContext ctx) throws OTimeoutException {
    if (!prev.isPresent()) {
      throw new IllegalStateException("Cannot calculate projections without a previous source");
    }

    OExecutionStream parentRs = prev.get().syncPull(ctx);
    return parentRs.map(this::mapResult);
  }

  private OResult mapResult(OResult result, OCommandContext ctx) {
    Object oldCurrent = ctx.getVariable("$current");
    ctx.setVariable("$current", result);
    OResult newResult = calculateProjections(ctx, result);
    ctx.setVariable("$current", oldCurrent);
    return newResult;
  }

  private OResult calculateProjections(OCommandContext ctx, OResult next) {
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      return this.projection.calculateSingle(ctx, next);
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);

    String result = spaces + "+ CALCULATE PROJECTIONS";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    result += ("\n" + spaces + "  " + projection.toString() + "");
    return result;
  }

  @Override
  public long getCost() {
    return cost;
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new ProjectionCalculationStep(projection.copy(), ctx, profilingEnabled);
  }
}
