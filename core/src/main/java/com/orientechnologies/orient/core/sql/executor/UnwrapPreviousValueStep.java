package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;

/**
 * for UPDATE, unwraps the current result set to return the previous value
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class UnwrapPreviousValueStep extends AbstractExecutionStep {

  public UnwrapPreviousValueStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OExecutionStream syncPull(OCommandContext ctx) throws OTimeoutException {
    OExecutionStream upstream = prev.get().syncPull(ctx);
    return attachProfile(upstream.map(this::mapResult));
  }

  private OResult mapResult(OResult result, OCommandContext ctx) {
    if (result instanceof OUpdatableResult) {
      result = ((OUpdatableResult) result).previousValue;
      if (result == null) {
        throw new OCommandExecutionException(
            "Invalid status of record: no previous value available");
      }
      return result;
    } else {
      throw new OCommandExecutionException("Invalid status of record: no previous value available");
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String result = OExecutionStepInternal.getIndent(depth, indent) + "+ UNWRAP PREVIOUS VALUE";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }
}
