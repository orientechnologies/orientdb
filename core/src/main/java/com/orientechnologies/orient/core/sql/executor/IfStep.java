package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OBooleanExpression;

/**
 * Created by luigidellaquila on 19/09/16.
 */
public class IfStep extends AbstractExecutionStep {
  OBooleanExpression   condition;
  OScriptExecutionPlan positivePlan;
  OScriptExecutionPlan negativePlan;

  Boolean conditionMet = null;

  public IfStep(OCommandContext ctx) {
    super(ctx);
  }

  @Override public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    init(ctx);
    if (conditionMet) {
      return positivePlan.fetchNext(nRecords);
    } else if (negativePlan != null) {
      return negativePlan.fetchNext(nRecords);
    } else {
      return new OInternalResultSet();
    }
  }

  protected void init(OCommandContext ctx) {
    if (conditionMet == null) {
      conditionMet = condition.evaluate((OResult) null, ctx);
    }
  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override public void sendResult(Object o, Status status) {

  }

  public OBooleanExpression getCondition() {
    return condition;
  }

  public void setCondition(OBooleanExpression condition) {
    this.condition = condition;
  }

  public OScriptExecutionPlan getPositivePlan() {
    return positivePlan;
  }

  public void setPositivePlan(OScriptExecutionPlan positivePlan) {
    this.positivePlan = positivePlan;
  }

  public OScriptExecutionPlan getNegativePlan() {
    return negativePlan;
  }

  public void setNegativePlan(OScriptExecutionPlan negativePlan) {
    this.negativePlan = negativePlan;
  }
}
