package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;

/**
 * Created by luigidellaquila on 06/07/16.
 */
public interface OExecutionStep extends OExecutionCallback{


  OTodoResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException;

  void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException;

  void sendTimeout();

  void addPrevious(OExecutionStep step);

  void setNext(OExecutionStep step);

  void close();
}
