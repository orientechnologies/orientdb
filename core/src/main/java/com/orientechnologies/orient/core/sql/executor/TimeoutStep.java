package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OTimeout;

/** Created by luigidellaquila on 08/08/16. */
public class TimeoutStep extends AbstractExecutionStep {
  private final OTimeout timeout;

  public TimeoutStep(OTimeout timeout) {
    super();
    this.timeout = timeout;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    return getPrev().get().start(ctx).timeout(timeout.getVal().longValue(), this::fail);
  }

  private void fail() {
    if (OTimeout.RETURN.equals(this.timeout.getFailureStrategy())) {
      return;
    } else {
      throw new OTimeoutException("Timeout expired");
    }
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    return OExecutionStepInternal.getIndent(ctx)
        + "+ TIMEOUT ("
        + timeout.getVal().toString()
        + " millis)";
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStepInternal copy(OCommandContext ctx) {
    return new TimeoutStep(this.timeout.copy());
  }
}
