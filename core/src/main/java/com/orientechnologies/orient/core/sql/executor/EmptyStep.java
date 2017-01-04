package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;

/**
 * Created by luigidellaquila on 08/07/16.
 */
public class EmptyStep extends AbstractExecutionStep {
  public EmptyStep(OCommandContext ctx) {
    super(ctx);
  }

  @Override public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    return new OInternalResultSet();
  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override public void sendResult(Object o, Status status) {

  }
}
