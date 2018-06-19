package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.*;

import java.util.List;

public class WhileStep extends AbstractExecutionStep {
  private final OBooleanExpression condition;
  private final List<OStatement>   statements;

  private OExecutionStepInternal finalResult = null;

  public WhileStep(OBooleanExpression condition, List<OStatement> statements, OCommandContext ctx, boolean enableProfiling) {
    super(ctx, enableProfiling);
    this.condition = condition;
    this.statements = statements;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    prev.ifPresent(x -> x.syncPull(ctx, nRecords));
    if (finalResult != null) {
      return finalResult.syncPull(ctx, nRecords);
    }

    while (condition.evaluate(new OResultInternal(), ctx)) {
      OScriptExecutionPlan plan = initPlan(ctx);
      OExecutionStepInternal result = plan.executeFull();
      if (result != null) {
        this.finalResult = result;
        return result.syncPull(ctx, nRecords);
      }
    }
    finalResult = new EmptyStep(ctx, false);
    return finalResult.syncPull(ctx, nRecords);

  }

  public OScriptExecutionPlan initPlan(OCommandContext ctx) {
    OBasicCommandContext subCtx1 = new OBasicCommandContext();
    subCtx1.setParent(ctx);
    OScriptExecutionPlan plan = new OScriptExecutionPlan(subCtx1);
    for (OStatement stm : statements) {
      plan.chain(stm.createExecutionPlan(subCtx1, profilingEnabled), profilingEnabled);
    }
    return plan;
  }

  public boolean containsReturn() {
    for (OStatement stm : this.statements) {
      if (stm instanceof OReturnStatement) {
        return true;
      }
      if (stm instanceof OForEachBlock && ((OForEachBlock) stm).containsReturn()) {
        return true;
      }
      if (stm instanceof OIfStatement && ((OIfStatement) stm).containsReturn()) {
        return true;
      }
      if (stm instanceof OWhileBlock && ((OWhileBlock) stm).containsReturn()) {
        return true;
      }
    }
    return false;
  }
}
