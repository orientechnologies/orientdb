package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OBooleanExpression;
import com.orientechnologies.orient.core.sql.parser.OIfStatement;
import com.orientechnologies.orient.core.sql.parser.OReturnStatement;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import java.util.List;

/** Created by luigidellaquila on 19/09/16. */
public class IfStep extends AbstractExecutionStep {
  protected OBooleanExpression condition;
  public List<OStatement> positiveStatements;
  public List<OStatement> negativeStatements;

  public IfStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    OScriptExecutionPlan plan = producePlan(ctx);
    if (plan != null) {
      return plan.start();
    } else {
      return OExecutionStream.empty();
    }
  }

  public OScriptExecutionPlan producePlan(OCommandContext ctx) {
    if (condition.evaluate((OResult) null, ctx)) {
      OScriptExecutionPlan positivePlan = initPositivePlan(ctx);
      return positivePlan;
    } else {
      OScriptExecutionPlan negativePlan = initNegativePlan(ctx);
      if (negativePlan != null) {
        return negativePlan;
      }
    }
    return null;
  }

  public OScriptExecutionPlan initPositivePlan(OCommandContext ctx) {
    OBasicCommandContext subCtx1 = new OBasicCommandContext();
    subCtx1.setParent(ctx);
    OScriptExecutionPlan positivePlan = new OScriptExecutionPlan(subCtx1);
    for (OStatement stm : positiveStatements) {
      positivePlan.chain(stm.createExecutionPlan(subCtx1, profilingEnabled), profilingEnabled);
    }
    return positivePlan;
  }

  public OScriptExecutionPlan initNegativePlan(OCommandContext ctx) {
    if (negativeStatements != null) {
      if (negativeStatements.size() > 0) {
        OBasicCommandContext subCtx2 = new OBasicCommandContext();
        subCtx2.setParent(ctx);
        OScriptExecutionPlan negativePlan = new OScriptExecutionPlan(subCtx2);
        for (OStatement stm : negativeStatements) {
          negativePlan.chain(stm.createExecutionPlan(subCtx2, profilingEnabled), profilingEnabled);
        }
        return negativePlan;
      }
    }
    return null;
  }

  public OBooleanExpression getCondition() {
    return condition;
  }

  public void setCondition(OBooleanExpression condition) {
    this.condition = condition;
  }

  public boolean containsReturn() {
    if (positiveStatements != null) {
      for (OStatement stm : positiveStatements) {
        if (containsReturn(stm)) {
          return true;
        }
      }
    }
    if (negativeStatements != null) {
      for (OStatement stm : negativeStatements) {
        if (containsReturn(stm)) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean containsReturn(OStatement stm) {
    if (stm instanceof OReturnStatement) {
      return true;
    }
    if (stm instanceof OIfStatement) {
      for (OStatement o : ((OIfStatement) stm).getStatements()) {
        if (containsReturn(o)) {
          return true;
        }
      }
    }
    return false;
  }
}
