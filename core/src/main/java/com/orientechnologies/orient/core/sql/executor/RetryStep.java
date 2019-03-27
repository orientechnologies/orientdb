package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.OExecutionThreadLocal;
import com.orientechnologies.orient.core.exception.OCommandInterruptedException;
import com.orientechnologies.orient.core.sql.parser.OStatement;

import java.util.Iterator;
import java.util.List;

/**
 * Created by luigidellaquila on 19/09/16.
 */
public class RetryStep extends AbstractExecutionStep {
  public        List<OStatement> body;
  private final int              retries;

  Iterator iterator;
  private OExecutionStepInternal finalResult = null;

  public RetryStep(List<OStatement> statements, int retries, OCommandContext ctx, boolean enableProfiling) {
    super(ctx, enableProfiling);
    this.body = statements;
    this.retries = retries;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    if (finalResult != null) {
      return finalResult.syncPull(ctx, nRecords);
    }
    for (int i = 0; i < retries; i++) {
      try {

        if (OExecutionThreadLocal.isInterruptCurrentOperation()) {
          throw new OCommandInterruptedException("The command has been interrupted");
        }
        OScriptExecutionPlan plan = initPlan(ctx);
        OExecutionStepInternal result = plan.executeFull();
        if (result != null) {
          this.finalResult = result;
          return result.syncPull(ctx, nRecords);
        }
        break;
      } catch (ONeedRetryException ex) {
        try {
          ctx.getDatabase().rollback();
        } catch (Exception e) {
        }

        if (i == retries - 1) {
          throw ex;
        }
      }
    }

    finalResult = new EmptyStep(ctx, false);
    return finalResult.syncPull(ctx, nRecords);

  }

  public OScriptExecutionPlan initPlan(OCommandContext ctx) {
    OBasicCommandContext subCtx1 = new OBasicCommandContext();
    subCtx1.setParent(ctx);
    OScriptExecutionPlan plan = new OScriptExecutionPlan(subCtx1);
    for (OStatement stm : body) {
      plan.chain(stm.createExecutionPlan(subCtx1, profilingEnabled), profilingEnabled);
    }
    return plan;
  }

}
