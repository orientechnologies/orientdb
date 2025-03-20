package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OTimeout;

/** Created by luigidellaquila on 08/08/16. */
public class AccumulatingTimeoutStep extends AbstractExecutionStep {

  private final OTimeout timeout;

  public AccumulatingTimeoutStep(OTimeout timeout) {
    super();
    this.timeout = timeout;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    final OExecutionStream internal = getPrev().get().start(ctx);
    return internal.timeout(this.timeout.getVal().longValue(), this::fail);
  }

  private void fail() {
    if (OTimeout.RETURN.equals(this.timeout.getFailureStrategy())) {
      // do nothing
    } else {
      throw new OTimeoutException("Timeout expired");
    }
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStepInternal copy(OCommandContext ctx) {
    return new AccumulatingTimeoutStep(timeout.copy());
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    return OExecutionStepInternal.getIndent(ctx)
        + "+ TIMEOUT ("
        + timeout.getVal().toString()
        + "ms)";
  }
}
