package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.OExecutionThreadLocal;
import com.orientechnologies.orient.core.exception.OCommandInterruptedException;
import com.orientechnologies.orient.core.sql.executor.AbstractExecutionStep;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.OScriptExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import java.util.List;

public class WhileStep extends AbstractExecutionStep {
  private final OBooleanExpression condition;
  private final List<OStatement> statements;

  public WhileStep(OBooleanExpression condition, List<OStatement> statements) {
    super();
    this.condition = condition;
    this.statements = statements;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    prev.ifPresent(x -> x.start(ctx).close(ctx));

    while (condition.evaluate(new OResultInternal(), ctx)) {
      if (OExecutionThreadLocal.isInterruptCurrentOperation())
        throw new OCommandInterruptedException("The command has been interrupted");

      OScriptExecutionPlan plan = initPlan(ctx);
      OExecutionStream result = plan.start(ctx);
      if (result.isTermination(ctx)) {
        return result;
      }
    }
    return OExecutionStream.empty();
  }

  public OScriptExecutionPlan initPlan(OCommandContext ctx) {
    OBasicCommandContext subCtx1 = new OBasicCommandContext(ctx.getDatabase());
    subCtx1.setParent(ctx);
    OScriptExecutionPlan plan = new OScriptExecutionPlan();
    for (OStatement stm : statements) {
      if (stm.originalStatement == null) {
        stm.originalStatement = stm.toString();
      }
      plan.chain(stm);
    }
    return plan;
  }
}
