package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OStatement;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 *     <p>This step represents the execution plan of an instruciton instide a batch script
 */
public class ScriptLineStep extends AbstractExecutionStep {
  protected final OStatement statement;
  private OInternalExecutionPlan plan;

  public ScriptLineStep(OStatement statement) {
    super();
    this.statement = statement;
  }

  private void initPlan(OCommandContext ctx) {
    if (plan == null) {
      plan = statement.createExecutionPlan(ctx);
    }
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    initPlan(ctx);
    return plan.start(ctx);
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    if (statement == null) {
      return "Script Line";
    }
    return statement.getOriginalStatement();
  }
}
