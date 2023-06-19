package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OSimpleExecStatement;

/** Created by luigidellaquila on 11/10/16. */
public class ReturnStep extends AbstractExecutionStep {
  private final OSimpleExecStatement statement;

  public ReturnStep(OSimpleExecStatement statement, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.statement = statement;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx) throws OTimeoutException {
    return statement.executeSimple(ctx);
  }
}
