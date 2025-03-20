package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;

/** Created by luigidellaquila on 20/02/17. */
public class CastToEdgeStep extends AbstractExecutionStep {

  public CastToEdgeStep() {
    super();
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    OExecutionStream upstream = getPrev().get().start(ctx);
    return upstream.map(this::mapResult);
  }

  private OResult mapResult(OResult result, OCommandContext ctx) {
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
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    String result = OExecutionStepInternal.getIndent(ctx) + "+ CAST TO EDGE";
    if (ctx.isProfilingEnabled()) {
      result += " (" + ctx.getCostFormatted(this) + ")";
    }
    return result;
  }

  @Override
  public OExecutionStepInternal copy(OCommandContext ctx) {
    return new CastToEdgeStep();
  }

  @Override
  public boolean canBeCached() {
    return true;
  }
}
