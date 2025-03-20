package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OBooleanExpression;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import java.util.List;

/** Created by luigidellaquila on 19/09/16. */
public class IfStep extends AbstractExecutionStep {
  protected OBooleanExpression condition;
  public List<OStatement> positiveStatements;
  public List<OStatement> negativeStatements;

  public IfStep() {
    super();
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    OScriptExecutionPlan plan = producePlan(ctx);
    if (plan != null) {
      return plan.start(ctx);
    } else {
      return OExecutionStream.empty();
    }
  }

  public OScriptExecutionPlan producePlan(OCommandContext ctx) {
    if (condition.evaluate((OResult) null, ctx)) {
      return initPositivePlan(ctx);
    } else {
      return initNegativePlan(ctx);
    }
  }

  public OScriptExecutionPlan initPositivePlan(OCommandContext ctx) {
    OBasicCommandContext subCtx1 = new OBasicCommandContext(ctx.getDatabase());
    subCtx1.setParent(ctx);
    OScriptExecutionPlan positivePlan = new OScriptExecutionPlan();
    for (OStatement stm : positiveStatements) {
      positivePlan.chain(stm);
    }
    return positivePlan;
  }

  public OScriptExecutionPlan initNegativePlan(OCommandContext ctx) {
    if (negativeStatements != null) {
      if (negativeStatements.size() > 0) {
        OBasicCommandContext subCtx2 = new OBasicCommandContext(ctx.getDatabase());
        subCtx2.setParent(ctx);
        OScriptExecutionPlan negativePlan = new OScriptExecutionPlan();
        for (OStatement stm : negativeStatements) {
          negativePlan.chain(stm);
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
}
