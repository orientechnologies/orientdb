package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;

/**
 * Created by luigidellaquila on 11/10/16.
 */
public class ReturnStep extends AbstractExecutionStep{
  public ReturnStep(OCommandContext ctx) {
    super(ctx);
  }

  @Override public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    return null;
  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override public void sendResult(Object o, Status status) {

  }
}
