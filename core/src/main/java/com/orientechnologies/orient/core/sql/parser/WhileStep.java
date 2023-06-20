package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.OExecutionThreadLocal;
import com.orientechnologies.orient.core.exception.OCommandInterruptedException;
import com.orientechnologies.orient.core.sql.executor.AbstractExecutionStep;
import com.orientechnologies.orient.core.sql.executor.EmptyStep;
import com.orientechnologies.orient.core.sql.executor.OExecutionStepInternal;
import com.orientechnologies.orient.core.sql.executor.OInternalExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.OScriptExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.List;

public class WhileStep extends AbstractExecutionStep {
  private final OBooleanExpression condition;
  private final List<OStatement> statements;

  public WhileStep(
      OBooleanExpression condition,
      List<OStatement> statements,
      OCommandContext ctx,
      boolean enableProfiling) {
    super(ctx, enableProfiling);
    this.condition = condition;
    this.statements = statements;
  }

  @Override
  public OExecutionStream syncPull(OCommandContext ctx) throws OTimeoutException {
    prev.ifPresent(x -> x.syncPull(ctx));

    while (condition.evaluate(new OResultInternal(), ctx)) {
      if (OExecutionThreadLocal.isInterruptCurrentOperation())
        throw new OCommandInterruptedException("The command has been interrupted");

      OScriptExecutionPlan plan = initPlan(ctx);
      OExecutionStepInternal result = plan.executeFull();
      if (result != null) {
        return result.syncPull(ctx);
      }
    }
    return new EmptyStep(ctx, false).syncPull(ctx);
  }

  public OScriptExecutionPlan initPlan(OCommandContext ctx) {
    OBasicCommandContext subCtx1 = new OBasicCommandContext();
    subCtx1.setParent(ctx);
    OScriptExecutionPlan plan = new OScriptExecutionPlan(subCtx1);
    for (OStatement stm : statements) {
      if (stm.originalStatement == null) {
        stm.originalStatement = stm.toString();
      }
      OInternalExecutionPlan subPlan;
      if (stm.originalStatement.contains("?")) {
        // cannot cache execution plans with positional parameters inside scripts
        subPlan = stm.createExecutionPlanNoCache(subCtx1, profilingEnabled);
      } else {
        subPlan = stm.createExecutionPlan(subCtx1, profilingEnabled);
      }
      plan.chain(subPlan, profilingEnabled);
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
