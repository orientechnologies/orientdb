package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OProjection;
import java.util.Map;
import java.util.Optional;

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
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    if (!prev.isPresent()) {
      throw new IllegalStateException("Cannot calculate projections without a previous source");
    }

    OResultSet parentRs = prev.get().syncPull(ctx, nRecords);
    return new OResultSet() {
      @Override
      public boolean hasNext() {
        return parentRs.hasNext();
      }

      @Override
      public OResult next() {
        OResult item = parentRs.next();
        Object oldCurrent = ctx.getVariable("$current");
        ctx.setVariable("$current", item);
        OResult result = calculateProjections(ctx, item);
        ctx.setVariable("$current", oldCurrent);
        return result;
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
