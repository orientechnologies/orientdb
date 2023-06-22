package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;

/** Created by luigidellaquila on 20/02/17. */
public class CastToVertexStep extends AbstractExecutionStep {

  public CastToVertexStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OExecutionStream syncPull(OCommandContext ctx) throws OTimeoutException {
    OExecutionStream upstream = getPrev().get().syncPull(ctx);
    return attachProfile(upstream.map(this::mapResult));
  }

  private OResult mapResult(OResult result, OCommandContext ctx) {
    if (result.getElement().orElse(null) instanceof OVertex) {
      return result;
    }
    if (result.isVertex()) {
      if (result instanceof OResultInternal) {
        ((OResultInternal) result).setElement(result.getElement().get().asVertex().get());
      } else {
        result = new OResultInternal(result.getElement().get().asVertex().get());
      }
    } else {
      throw new OCommandExecutionException("Current element is not a vertex: " + result);
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String result = OExecutionStepInternal.getIndent(depth, indent) + "+ CAST TO VERTEX";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }
}
