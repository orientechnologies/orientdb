package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luigidellaquila on 06/07/16.
 */
public class OSelectExecutionPlan implements OInternalExecutionPlan {

  private final OCommandContext ctx;

  protected List<OExecutionStep> steps = new ArrayList<>();
  OExecutionStep lastStep = null;

  public OSelectExecutionPlan(OCommandContext ctx) {
    this.ctx = ctx;
  }

  @Override public void close() {
    lastStep.close();
  }

  @Override public OTodoResultSet fetchNext(int n) {
    return lastStep.syncPull(ctx, n);
  }

  @Override public String prettyPrint(int indent) {
    StringBuilder result = new StringBuilder();
    for (OExecutionStep step : steps) {
      result.append(step.prettyPrint(0, indent));
      result.append("\n");
    }
    return result.toString();
  }

  @Override public void reset(OCommandContext ctx) {
    throw new UnsupportedOperationException();
  }

  protected void chain(OExecutionStep nextStep) {
    if (lastStep != null) {
      lastStep.setNext(nextStep);
      nextStep.setPrevious(lastStep);
    }
    lastStep = nextStep;
    steps.add(nextStep);
  }
}
