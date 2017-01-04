package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OBinaryCondition;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;

/**
 * Created by luigidellaquila on 06/08/16.
 */
public class FetchFromIndexedFunctionStep extends AbstractExecutionStep {
  private final OBinaryCondition functionCondition;
  private final OIdentifier      queryTarget;

  public FetchFromIndexedFunctionStep(OBinaryCondition functionCondition, OIdentifier queryTarget, OCommandContext ctx) {
    super(ctx);
    this.functionCondition = functionCondition;
    this.queryTarget = queryTarget;
  }

  @Override public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    return null;//TODO
  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override public void sendResult(Object o, Status status) {

  }
}
