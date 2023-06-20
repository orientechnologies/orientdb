package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;

/**
 * Counts the records from the previous steps. Returns a record with a single property, called
 * "count" containing the count of records received from pervious steps
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class CountStep extends AbstractExecutionStep {

  private long cost = 0;

  /**
   * @param ctx the query context
   * @param profilingEnabled true to enable the profiling of the execution (for SQL PROFILE)
   */
  public CountStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OExecutionStream syncPull(OCommandContext ctx) throws OTimeoutException {
    long count = 0;
    OExecutionStream prevResult = getPrev().get().syncPull(ctx);
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      while (prevResult.hasNext(ctx)) {
        count++;
        prevResult.next(ctx);
      }
      OResultInternal resultRecord = new OResultInternal();
      resultRecord.setProperty("count", count);
      return OExecutionStream.singleton(resultRecord);
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ COUNT");
    if (profilingEnabled) {
      result.append(" (" + getCostFormatted() + ")");
    }
    return result.toString();
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
    return new CountStep(ctx, profilingEnabled);
  }
}
