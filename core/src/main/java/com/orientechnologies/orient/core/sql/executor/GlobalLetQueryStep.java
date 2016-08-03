package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import com.orientechnologies.orient.core.sql.parser.OStatement;

/**
 * Created by luigidellaquila on 03/08/16.
 */
public class GlobalLetQueryStep extends AbstractExecutionStep {

  private final OIdentifier varName;
  private final OStatement  query;

  public GlobalLetQueryStep(OIdentifier varName, OStatement query, OCommandContext ctx) {
    super(ctx);
    this.varName = varName;
    this.query = query;
  }

  @Override public OTodoResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    return null;
  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override public void sendResult(Object o, Status status) {

  }
}
