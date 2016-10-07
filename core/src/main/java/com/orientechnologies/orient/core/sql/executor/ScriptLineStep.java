package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 *         <p>
 *         This step represents the execution plan of an instruciton instide a batch script
 */
public class ScriptLineStep extends AbstractExecutionStep {
  private final OInternalExecutionPlan plan;

  public ScriptLineStep(OInternalExecutionPlan nextPlan, OCommandContext ctx) {
    super(ctx);
    this.plan = nextPlan;
  }

  @Override public OTodoResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    return plan.fetchNext(nRecords);
  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override public void sendResult(Object o, Status status) {

  }

  public boolean containsReturn() {
    if(plan instanceof OScriptExecutionPlan){
      return ((OScriptExecutionPlan)plan).containsReturn();
    }
    return false;
  }

  public OExecutionStepInternal executeUntilReturn(OCommandContext ctx) {
    if(plan instanceof OScriptExecutionPlan){
      return ((OScriptExecutionPlan)plan).executeUntilReturn();
    }
    throw new IllegalStateException();
  }
}
