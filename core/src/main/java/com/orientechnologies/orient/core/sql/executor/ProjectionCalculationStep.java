package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OProjection;

/** Created by luigidellaquila on 12/07/16. */
public class ProjectionCalculationStep extends AbstractExecutionStep {
  protected final OProjection projection;

  public ProjectionCalculationStep(OProjection projection) {
    super();
    this.projection = projection;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    if (!prev.isPresent()) {
      throw new IllegalStateException("Cannot calculate projections without a previous source");
    }

    OExecutionStream parentRs = prev.get().start(ctx);
    return parentRs.map(this::mapResult);
  }

  private OResult mapResult(OResult result, OCommandContext ctx) {
    OResult oldCurrent = ctx.getCurrent();
    ctx.setCurrent(result);
    OResult newResult = calculateProjections(ctx, result);
    ctx.setCurrent(oldCurrent);
    return newResult;
  }

  private OResult calculateProjections(OCommandContext ctx, OResult next) {
    return this.projection.calculateSingle(ctx, next);
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    String spaces = OExecutionStepInternal.getIndent(ctx);

    String result = spaces + "+ CALCULATE PROJECTIONS";
    if (ctx.isProfilingEnabled()) {
      result += " (" + ctx.getCostFormatted(this) + ")";
    }
    result += ("\n" + spaces + "  " + projection.toString() + "");
    return result;
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStepInternal copy(OCommandContext ctx) {
    return new ProjectionCalculationStep(projection.copy());
  }
}
