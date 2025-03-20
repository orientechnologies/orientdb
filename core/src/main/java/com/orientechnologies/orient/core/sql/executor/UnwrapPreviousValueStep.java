package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;

/**
 * for UPDATE, unwraps the current result set to return the previous value
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class UnwrapPreviousValueStep extends AbstractExecutionStep {

  public UnwrapPreviousValueStep() {
    super();
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    OExecutionStream upstream = prev.get().start(ctx);
    return upstream.map(this::mapResult);
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
  public String prettyPrint(OPrintContext ctx) {
    String result = OExecutionStepInternal.getIndent(ctx) + "+ UNWRAP PREVIOUS VALUE";
    if (ctx.isProfilingEnabled()) {
      result += " (" + ctx.getCostFormatted(this) + ")";
    }
    return result;
  }
}
