package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;

/** Created by luigidellaquila on 08/07/16. */
public class EmptyStep extends AbstractExecutionStep {
  public EmptyStep() {
    super();
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.start(ctx).close(ctx));
    return OExecutionStream.empty();
  }

  public OExecutionStepInternal copy(OCommandContext ctx) {
    throw new UnsupportedOperationException();
  }

  public boolean canBeCached() {
    return false;
    // DON'T TOUCH!
    // This step is there most of the cases because the query was early optimized based on DATA, eg.
    // an empty cluster,
    // so this execution plan cannot be cached!!!
  }
}
