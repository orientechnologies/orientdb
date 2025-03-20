package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OLimit;

/** Created by luigidellaquila on 08/07/16. */
public class LimitExecutionStep extends AbstractExecutionStep {
  private final OLimit limit;

  public LimitExecutionStep(OLimit limit) {
    super();
    this.limit = limit;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    int limitVal = limit.getValue(ctx);
    if (limitVal == -1) {
      return getPrev().get().start(ctx);
    }
    OExecutionStream result = prev.get().start(ctx);
    return result.limit(limitVal);
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    return OExecutionStepInternal.getIndent(ctx) + "+ LIMIT (" + limit.toString() + ")";
  }
}
