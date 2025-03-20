package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;

/**
 * Counts the records from the previous steps. Returns a record with a single property, called
 * "count" containing the count of records received from pervious steps
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class CountStep extends AbstractExecutionStep {

  /**
   */
  public CountStep() {
    super();
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    OExecutionStream prevResult = getPrev().get().start(ctx);
    long count = 0;
    while (prevResult.hasNext(ctx)) {
      count++;
      prevResult.next(ctx);
    }
    prevResult.close(ctx);
    OResultInternal resultRecord = new OResultInternal();
    resultRecord.setProperty("count", count);
    return OExecutionStream.singleton(resultRecord);
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    String spaces = OExecutionStepInternal.getIndent(ctx);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ COUNT");
    if (ctx.isProfilingEnabled()) {
      result.append(" (" + ctx.getCostFormatted(this) + ")");
    }
    return result.toString();
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStepInternal copy(OCommandContext ctx) {
    return new CountStep();
  }
}
