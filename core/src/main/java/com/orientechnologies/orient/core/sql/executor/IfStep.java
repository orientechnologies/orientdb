package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OBooleanExpression;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import java.util.List;

/** Created by luigidellaquila on 19/09/16. */
public class IfStep extends AbstractExecutionStep {
  protected OBooleanExpression condition;
  protected OScriptExecutionPlan positivePlan;
  protected OScriptExecutionPlan negativePlan;

  private Boolean conditionMet = null;
  public List<OStatement> positiveStatements;
  public List<OStatement> negativeStatements;

  public IfStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    init(ctx);
    if (conditionMet) {
      initPositivePlan(ctx);
      return positivePlan.fetchNext(nRecords);
    } else {
      initNegativePlan(ctx);
      if (negativePlan != null) {
        return negativePlan.fetchNext(nRecords);
      }
    }
    return new OInternalResultSet();
  }

  protected void init(OCommandContext ctx) {
    if (conditionMet == null) {
      conditionMet = condition.evaluate((OResult) null, ctx);
    }
  }

  public void initPositivePlan(OCommandContext ctx) {
    if (positivePlan == null) {
      OBasicCommandContext subCtx1 = new OBasicCommandContext();
      subCtx1.setParent(ctx);
      OScriptExecutionPlan positivePlan = new OScriptExecutionPlan(subCtx1);
      for (OStatement stm : positiveStatements) {
        positivePlan.chain(stm.createExecutionPlan(subCtx1, profilingEnabled), profilingEnabled);
      }
      setPositivePlan(positivePlan);
    }
  }

  public void initNegativePlan(OCommandContext ctx) {
    if (negativePlan == null && negativeStatements != null) {
      if (negativeStatements.size() > 0) {
        OBasicCommandContext subCtx2 = new OBasicCommandContext();
        subCtx2.setParent(ctx);
        OScriptExecutionPlan negativePlan = new OScriptExecutionPlan(subCtx2);
        for (OStatement stm : negativeStatements) {
          negativePlan.chain(stm.createExecutionPlan(subCtx2, profilingEnabled), profilingEnabled);
        }
        setNegativePlan(negativePlan);
      }
    }
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
