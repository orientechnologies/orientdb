package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;

/** Created by luigidellaquila on 20/02/17. */
public class CastToEdgeStep extends AbstractExecutionStep {

  private long cost = 0;

  public CastToEdgeStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OExecutionStream syncPull(OCommandContext ctx) throws OTimeoutException {
    OExecutionStream upstream = getPrev().get().syncPull(ctx);
    return upstream.map(this::mapResult);
  }

  private OResult mapResult(OResult result, OCommandContext ctx) {
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      if (result.getElement().orElse(null) instanceof OEdge) {
        return result;
      }
      if (result.isEdge()) {
        if (result instanceof OResultInternal) {
          ((OResultInternal) result).setElement(result.getElement().get().asEdge().get());
        } else {
          result = new OResultInternal(result.getElement().get().asEdge().get());
        }
      } else {
        throw new OCommandExecutionException("Current element is not a vertex: " + result);
      }
      return result;
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String result = OExecutionStepInternal.getIndent(depth, indent) + "+ CAST TO EDGE";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

  @Override
  public long getCost() {
    return cost;
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new CastToEdgeStep(ctx, profilingEnabled);
  }

  @Override
  public boolean canBeCached() {
    return true;
  }
}
