package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;

/** Created by luigidellaquila on 08/07/16. */
public class EmptyStep extends AbstractExecutionStep {
  public EmptyStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    OInternalResultSet result = new OInternalResultSet();
    return result;
  }

  public OExecutionStep copy(OCommandContext ctx) {
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
