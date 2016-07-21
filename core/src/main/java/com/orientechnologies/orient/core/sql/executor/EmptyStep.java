package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;

/**
 * Created by luigidellaquila on 08/07/16.
 */
public class EmptyStep implements OExecutionStepInternal {
  @Override public OTodoResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    return new OInternalResultSet();
  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override public void sendTimeout() {

  }

  @Override public void setPrevious(OExecutionStepInternal step) {

  }

  @Override public void setNext(OExecutionStepInternal step) {

  }

  @Override public void close() {

  }

  @Override public void sendResult(Object o, Status status) {

  }
}
