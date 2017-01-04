package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;

/**
 * Created by luigidellaquila on 22/07/16.
 */
public class SubQueryStep extends AbstractExecutionStep {
  private final OInternalExecutionPlan subExecuitonPlan;
  private final OCommandContext   childCtx;

  /**
   * executes a sub-query
   *
   * @param subExecutionPlan the execution plan of the sub-query
   * @param ctx              the context of the current execution plan
   * @param subCtx           the context of the subquery execution plan
   */
  public SubQueryStep(OInternalExecutionPlan subExecutionPlan, OCommandContext ctx, OCommandContext subCtx) {
    super(ctx);
    this.subExecuitonPlan = subExecutionPlan;
    this.childCtx = subCtx;
  }

  @Override public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    return subExecuitonPlan.fetchNext(nRecords);
  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {
    //TODO
  }

  @Override public void sendResult(Object o, Status status) {
    //TODO
  }

  @Override public String prettyPrint(int depth, int indent) {
    StringBuilder builder = new StringBuilder();
    String ind = OExecutionStepInternal.getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ FETCH FROM SUBQUERY \n");
    builder.append(subExecuitonPlan.prettyPrint(depth + 1, indent));
    return builder.toString();
  }
}
