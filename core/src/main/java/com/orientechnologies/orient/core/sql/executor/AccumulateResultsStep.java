package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;

/**
 * Drains the previous step's stream into memory, closes it, and emits from the collected copy.
 * Issue #10913: UPDATE pulls from an index range iterator one row at a time, mutating each row
 * before the next pull. When the mutation moves the indexed value forward inside the range,
 * the still-open iterator re-visits the row. Materializing the source RID list before any
 * mutation starts breaks this feedback loop.
 */
public class AccumulateResultsStep extends AbstractExecutionStep {

  public AccumulateResultsStep() {
    super();
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    if (!prev.isPresent()) {
      throw new IllegalStateException("accumulate step requires a previous step");
    }
    OExecutionStream resultSet = prev.get().start(ctx);
    return OExecutionStream.collectAll(resultSet, ctx);
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    String result = OExecutionStepInternal.getIndent(ctx) + "+ ACCUMULATE RESULTS";
    if (ctx.isProfilingEnabled()) {
      result += " (" + ctx.getCostFormatted(this) + ")";
    }
    return result;
  }
}
