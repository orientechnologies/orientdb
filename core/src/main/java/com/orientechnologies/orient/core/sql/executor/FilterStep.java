package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;

/**
 * Created by luigidellaquila on 12/07/16.
 */
public class FilterStep extends AbstractExecutionStep {
  private final OWhereClause whereClause;

  public FilterStep(OWhereClause whereClause, OCommandContext ctx) {
    super(ctx);
    this.whereClause = whereClause;
  }

  @Override public OTodoResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    if(!prev.isPresent()){
      throw new IllegalStateException("filter step requires a previous step");
    }
    OExecutionStep prevStep = prev.get();

    return null;
  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override public void sendResult(Object o, Status status) {

  }
}
