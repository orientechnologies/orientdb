package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;

/**
 * Created by luigidellaquila on 21/02/17.
 */
public class FetchEdgesToVerticesStep extends AbstractExecutionStep {
  public FetchEdgesToVerticesStep(String toAlias, OIdentifier targetClass, OIdentifier targetCluster, OCommandContext ctx) {
    super(ctx);
    //TODO
  }

  @Override
  public void sendResult(Object o, Status status) {

  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    throw new UnsupportedOperationException();//TODO
  }

  @Override
  public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }
}
