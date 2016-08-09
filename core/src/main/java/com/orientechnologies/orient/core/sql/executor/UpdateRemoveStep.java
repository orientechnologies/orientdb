package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OUpdateRemoveItem;

import java.util.List;

/**
 * Created by luigidellaquila on 09/08/16.
 */
public class UpdateRemoveStep extends AbstractExecutionStep{
  public UpdateRemoveStep(List<OUpdateRemoveItem> updateRemoveItems, OCommandContext ctx) {
    super(ctx);
  }

  @Override public OTodoResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    throw new UnsupportedOperationException("Implement "+getClass().getSimpleName());
  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override public void sendResult(Object o, Status status) {

  }
}
