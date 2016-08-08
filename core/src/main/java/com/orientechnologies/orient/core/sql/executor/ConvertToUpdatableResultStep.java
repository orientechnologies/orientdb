package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;

/**
 * takes a normal result set and transforms it in another result set made of OUpdatableRecord instances.
 * Records that are not identifiable are discarded.
 *
 * @author Luigi Dell'Aquila
 */
public class ConvertToUpdatableResultStep extends AbstractExecutionStep {
  public ConvertToUpdatableResultStep(OCommandContext ctx) {
    super(ctx);
  }

  @Override public OTodoResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    return null;
  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override public void sendResult(Object o, Status status) {

  }
}
