package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luigidellaquila on 06/07/16.
 */
public class OSelectExecutionPlan implements OInternalExecutionPlan {

  private String location;

  private final OCommandContext ctx;

  protected List<OExecutionStepInternal> steps = new ArrayList<>();

  OExecutionStepInternal lastStep = null;

  public OSelectExecutionPlan(OCommandContext ctx) {
    this.ctx = ctx;
  }

  @Override public void close() {
    lastStep.close();
  }

  @Override public OTodoResultSet fetchNext(int n) {
    return lastStep.syncPull(ctx, n);
  }

  @Override public String prettyPrint(int depth, int indent) {
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < steps.size(); i++) {
      OExecutionStepInternal step = steps.get(i);
      result.append(step.prettyPrint(depth, indent));
      if (i < steps.size() - 1) {
        result.append("\n");
      }
    }
    return result.toString();
  }

  @Override public void reset(OCommandContext ctx) {
    throw new UnsupportedOperationException();
  }

  protected void chain(OExecutionStepInternal nextStep) {
    if (lastStep != null) {
      lastStep.setNext(nextStep);
      nextStep.setPrevious(lastStep);
    }
    lastStep = nextStep;
    steps.add(nextStep);
  }

  @Override public List<OExecutionStep> getSteps() {
    //TODO do a copy of the steps
    return (List) steps;
  }

  @Override public OResult toResult() {
    return new OResultInternal();
  }

  @Override public long getCost() {
    return 0l;
  }
}

