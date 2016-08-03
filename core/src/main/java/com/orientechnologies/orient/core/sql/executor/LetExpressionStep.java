package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;

/**
 * Created by luigidellaquila on 03/08/16.
 */
public class LetExpressionStep extends AbstractExecutionStep {
  private final OIdentifier varname;
  private final OExpression expression;

  public LetExpressionStep(OIdentifier varName, OExpression expression, OCommandContext ctx) {
    super(ctx);
    this.varname = varName;
    this.expression = expression;
  }

  @Override public OTodoResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    return null;
  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override public void sendResult(Object o, Status status) {

  }
}
