package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OTimeoutResultSet;
import com.orientechnologies.orient.core.sql.parser.OTimeout;

/** Created by luigidellaquila on 08/08/16. */
public class AccumulatingTimeoutStep extends AbstractExecutionStep {

  private final OTimeout timeout;

  public AccumulatingTimeoutStep(OTimeout timeout, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.timeout = timeout;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx) throws OTimeoutException {
    final OResultSet internal = getPrev().get().syncPull(ctx);
    return new OTimeoutResultSet(internal, this.timeout.getVal().longValue(), this::fail);
  }

  private void fail() {
    this.timedOut = true;
    if (OTimeout.RETURN.equals(this.timeout.getFailureStrategy())) {
      // do nothing
    } else {
      sendTimeout();
      throw new OTimeoutException("Timeout expired");
    }
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new AccumulatingTimeoutStep(timeout.copy(), ctx, profilingEnabled);
  }

  @Override
  public void reset() {}

  @Override
  public String prettyPrint(int depth, int indent) {
    return OExecutionStepInternal.getIndent(depth, indent)
        + "+ TIMEOUT ("
        + timeout.getVal().toString()
        + "ms)";
  }
}
