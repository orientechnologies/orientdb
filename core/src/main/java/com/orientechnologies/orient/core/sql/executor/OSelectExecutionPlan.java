package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Created by luigidellaquila on 06/07/16. */
public class OSelectExecutionPlan implements OInternalExecutionPlan {

  private String location;

  private OCommandContext ctx;

  protected List<OExecutionStepInternal> steps = new ArrayList<>();

  private OExecutionStepInternal lastStep = null;

  private String statement;
  private String genericStatement;

  public OSelectExecutionPlan(OCommandContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public void close() {
    lastStep.close();
  }

  @Override
  public OResultSet fetchNext(int n) {
    return lastStep.syncPull(ctx, n);
  }

  @Override
  public String prettyPrint(int depth, int indent) {
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

  @Override
  public void reset(OCommandContext ctx) {
    steps.forEach(OExecutionStepInternal::reset);
  }

  public void chain(OExecutionStepInternal nextStep) {
    if (lastStep != null) {
      lastStep.setNext(nextStep);
      nextStep.setPrevious(lastStep);
    }
    lastStep = nextStep;
    steps.add(nextStep);
  }

  @Override
  public List<OExecutionStep> getSteps() {
    // TODO do a copy of the steps
    return (List) steps;
  }

  public void setSteps(List<OExecutionStepInternal> steps) {
    this.steps = steps;
    if (steps.size() > 0) {
      lastStep = steps.get(steps.size() - 1);
    } else {
      lastStep = null;
    }
  }

  @Override
  public OResult toResult() {
    OResultInternal result = new OResultInternal();
    result.setProperty("type", "QueryExecutionPlan");
    result.setProperty(JAVA_TYPE, getClass().getName());
    result.setProperty("cost", getCost());
    result.setProperty("prettyPrint", prettyPrint(0, 2));
    result.setProperty(
        "steps",
        steps == null ? null : steps.stream().map(x -> x.toResult()).collect(Collectors.toList()));
    return result;
  }

  @Override
  public long getCost() {
    return 0l;
  }

  public OResult serialize() {
    OResultInternal result = new OResultInternal();
    result.setProperty("type", "QueryExecutionPlan");
    result.setProperty(JAVA_TYPE, getClass().getName());
    result.setProperty("cost", getCost());
    result.setProperty("prettyPrint", prettyPrint(0, 2));
    result.setProperty(
        "steps",
        steps == null ? null : steps.stream().map(x -> x.serialize()).collect(Collectors.toList()));
    return result;
  }

  public void deserialize(OResult serializedExecutionPlan) {
    List<OResult> serializedSteps = serializedExecutionPlan.getProperty("steps");
    for (OResult serializedStep : serializedSteps) {
      try {
        String className = serializedStep.getProperty(JAVA_TYPE);
        OExecutionStepInternal step =
            (OExecutionStepInternal) Class.forName(className).newInstance();
        step.deserialize(serializedStep);
        chain(step);
      } catch (Exception e) {
        throw OException.wrapException(
            new OCommandExecutionException("Cannot deserialize execution step:" + serializedStep),
            e);
      }
    }
  }

  @Override
  public OInternalExecutionPlan copy(OCommandContext ctx) {
    OSelectExecutionPlan copy = new OSelectExecutionPlan(ctx);
    copyOn(copy, ctx);
    return copy;
  }

  protected void copyOn(OSelectExecutionPlan copy, OCommandContext ctx) {
    OExecutionStep lastStep = null;
    for (OExecutionStep step : this.steps) {
      OExecutionStepInternal newStep =
          (OExecutionStepInternal) ((OExecutionStepInternal) step).copy(ctx);
      newStep.setPrevious((OExecutionStepInternal) lastStep);
      if (lastStep != null) {
        ((OExecutionStepInternal) lastStep).setNext(newStep);
      }
      lastStep = newStep;
      copy.getSteps().add(newStep);
    }
    copy.lastStep = copy.steps.size() == 0 ? null : copy.steps.get(copy.steps.size() - 1);
    copy.location = this.location;
    copy.statement = this.statement;
  }

  @Override
  public boolean canBeCached() {
    for (OExecutionStepInternal step : steps) {
      if (!step.canBeCached()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String getStatement() {
    return statement;
  }

  @Override
  public void setStatement(String statement) {
    this.statement = statement;
  }

  @Override
  public String getGenericStatement() {
    return this.genericStatement;
  }

  @Override
  public void setGenericStatement(String stm) {
    this.genericStatement = stm;
  }
}
