package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.OExecutionThreadLocal;
import com.orientechnologies.orient.core.exception.OCommandInterruptedException;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import java.util.List;

/** Created by luigidellaquila on 19/09/16. */
public class RetryStep extends AbstractExecutionStep {
  public List<OStatement> body;
  public List<OStatement> elseBody;
  public boolean elseFail;
  private final int retries;

  public RetryStep(
      List<OStatement> statements,
      int retries,
      List<OStatement> elseStatements,
      Boolean elseFail,
      OCommandContext ctx,
      boolean enableProfiling) {
    super(ctx, enableProfiling);
    this.body = statements;
    this.retries = retries;
    this.elseBody = elseStatements;
    this.elseFail = !(Boolean.FALSE.equals(elseFail));
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.start(ctx).close(ctx));
    for (int i = 0; i < retries; i++) {
      try {

        if (OExecutionThreadLocal.isInterruptCurrentOperation()) {
          throw new OCommandInterruptedException("The command has been interrupted");
        }
        OScriptExecutionPlan plan = initPlan(body, ctx);
        OExecutionStepInternal result = plan.executeFull();
        if (result != null) {
          return result.start(ctx);
        }
        break;
      } catch (ONeedRetryException ex) {
        try {
          ctx.getDatabase().rollback();
        } catch (Exception e) {
        }

        if (i == retries - 1) {
          if (elseBody != null && elseBody.size() > 0) {
            OScriptExecutionPlan plan = initPlan(elseBody, ctx);
            OExecutionStepInternal result = plan.executeFull();
            if (result != null) {
              return result.start(ctx);
            }
          }
          if (elseFail) {
            throw ex;
          } else {
            return OExecutionStream.empty();
          }
        }
      }
    }

    return new EmptyStep(ctx, false).start(ctx);
  }

  public OScriptExecutionPlan initPlan(List<OStatement> body, OCommandContext ctx) {
    OBasicCommandContext subCtx1 = new OBasicCommandContext(ctx.getDatabase());
    subCtx1.setParent(ctx);
    OScriptExecutionPlan plan = new OScriptExecutionPlan(subCtx1);
    for (OStatement stm : body) {
      plan.chain(stm.createExecutionPlan(subCtx1, profilingEnabled), profilingEnabled);
    }
    return plan;
  }
}
