package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OTimeout;

/** Created by luigidellaquila on 08/08/16. */
public class TimeoutStep extends AbstractExecutionStep {
  private final OTimeout timeout;

  private Long expiryTime;

  public TimeoutStep(OTimeout timeout, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.timeout = timeout;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    if (this.expiryTime == null) {
      this.expiryTime = System.currentTimeMillis() + timeout.getVal().longValue();
    }
    if (System.currentTimeMillis() > expiryTime) {
      return fail();
    }
    return getPrev().get().syncPull(ctx, nRecords); // TODO do it more granular
  }

  private OResultSet fail() {
    this.timedOut = true;
    sendTimeout();
    if (OTimeout.RETURN.equals(this.timeout.getFailureStrategy())) {
      return new OInternalResultSet();
    } else {
      throw new OTimeoutException("Timeout expired");
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    return OExecutionStepInternal.getIndent(depth, indent)
        + "+ TIMEOUT ("
        + timeout.getVal().toString()
        + " millis)";
  }

  @Override
  public void reset() {
    expiryTime = null;
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new TimeoutStep(this.timeout.copy(), ctx, profilingEnabled);
  }
}
