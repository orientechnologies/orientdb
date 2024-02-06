package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OLimit;

/** Created by luigidellaquila on 08/07/16. */
public class LimitExecutionStep extends AbstractExecutionStep {
  private final OLimit limit;

  public LimitExecutionStep(OLimit limit, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
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
  public void sendTimeout() {}

  @Override
  public void close() {
    prev.ifPresent(x -> x.close());
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    return OExecutionStepInternal.getIndent(depth, indent) + "+ LIMIT (" + limit.toString() + ")";
  }
}
