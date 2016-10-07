package com.orientechnologies.orient.core.sql.executor;

/**
 * Created by luigidellaquila on 08/08/16.
 */

import com.orientechnologies.orient.core.command.OCommandContext;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OScriptExecutionPlan implements OInternalExecutionPlan {

  private String location;

  private final OCommandContext ctx;

  protected List<ScriptLineStep> steps = new ArrayList<>();

  OExecutionStepInternal lastStep = null;

  public OScriptExecutionPlan(OCommandContext ctx) {
    this.ctx = ctx;
  }

  @Override public void reset(OCommandContext ctx) {
    //TODO
    throw new UnsupportedOperationException();
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

  public void chain(OInternalExecutionPlan nextPlan) {
    ScriptLineStep lastStep = steps.size() == 0 ? null : steps.get(steps.size() - 1);
    ScriptLineStep nextStep = new ScriptLineStep(nextPlan, ctx);
    if (lastStep != null) {
      lastStep.setNext(nextStep);
      nextStep.setPrevious(lastStep);
    }
    steps.add(nextStep);
    this.lastStep = nextStep;
  }

  @Override public List<OExecutionStep> getSteps() {
    //TODO do a copy of the steps
    return (List) steps;
  }

  public void setSteps(List<OExecutionStepInternal> steps) {
    this.steps = (List) steps;
  }

  @Override public OResult toResult() {
    return new OResultInternal();
  }

  @Override public long getCost() {
    return 0l;
  }

  public boolean containsReturn() {
    for (OExecutionStepInternal step : steps) {
      if (step instanceof ReturnStep) {
        return true;
      }
      if (step instanceof ScriptLineStep) {
        return ((ScriptLineStep) step).containsReturn();
      }
    }

    return false;
  }

  public OExecutionStepInternal executeUntilReturn() {
    if (steps.size() > 0) {
      lastStep = steps.get(steps.size() - 1);
    }
    for (int i = 0; i < steps.size() - 1; i++) {
      ScriptLineStep step = steps.get(i);
      if (step.containsReturn()) {
        OExecutionStepInternal returnStep = step.executeUntilReturn(ctx);
        if (returnStep != null) {
          lastStep = returnStep;
          return lastStep;
        }
      }
      OTodoResultSet lastResult = step.syncPull(ctx, 100);

      while (lastResult.hasNext()) {
        while (lastResult.hasNext()) {
          lastResult.next();
        }
        lastResult = step.syncPull(ctx, 100);
      }
    }
    this.lastStep = steps.get(steps.size() - 1);
    return lastStep;
  }
}

