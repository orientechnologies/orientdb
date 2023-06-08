package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.resultset.OResultSetMapper;

/**
 * for UPDATE, unwraps the current result set to return the previous value
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class UnwrapPreviousValueStep extends AbstractExecutionStep {

  private long cost = 0;

  public UnwrapPreviousValueStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    OResultSet upstream = prev.get().syncPull(ctx);
    return new OResultSetMapper(upstream, this::mapResult);
  }

  private OResult mapResult(OResult result) {
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      if (result instanceof OUpdatableResult) {
        result = ((OUpdatableResult) result).previousValue;
        if (result == null) {
          throw new OCommandExecutionException(
              "Invalid status of record: no previous value available");
        }
        return result;
      } else {
        throw new OCommandExecutionException(
            "Invalid status of record: no previous value available");
      }
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
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

  @Override
  public long getCost() {
    return cost;
  }
}
