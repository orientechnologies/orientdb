package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;

import java.util.List;

/**
 * Created by luigidellaquila on 11/10/16.
 */
public class CartesianProductStep extends AbstractExecutionStep {

  private List<OExecutionPlan> subPlans;

  public CartesianProductStep(OCommandContext ctx) {
    super(ctx);
  }

  @Override public OTodoResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    throw new UnsupportedOperationException("cartesian product is not yet implemented in MATCH statement");
    //TODO
  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override public void sendResult(Object o, Status status) {

  }

  public void addSubPlan(OExecutionPlan subPlan) {
    this.subPlans.add(subPlan);
  }
}
