package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Created by luigidellaquila on 06/07/16. */
public class OSelectExecutionPlan implements OInternalExecutionPlan {

  protected List<OExecutionStepInternal> steps = new ArrayList<>();

  private OExecutionStepInternal lastStep = null;

  private String statement;
  private String genericStatement;

  public OSelectExecutionPlan() {}

  @Override
  public OExecutionStream start(OCommandContext ctx) {
    return lastStep.start(ctx);
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < steps.size(); i++) {
      OExecutionStepInternal step = steps.get(i);
      result.append(step.prettyPrint(ctx));
      if (i < steps.size() - 1) {
        result.append("\n");
      }
    }
    return result.toString();
  }

  public void chain(OExecutionStepInternal nextStep) {
    if (lastStep != null) {
      nextStep.setPrevious(lastStep);
    }
    lastStep = nextStep;
    steps.add(nextStep);
  }

  @Override
  public List<OExecutionStepInternal> getSteps() {
    // TODO do a copy of the steps
    return steps;
  }

  public void setSteps(List<OExecutionStepInternal> steps) {
    this.steps = steps;
    if (steps.size() > 0) {
      lastStep = steps.get(steps.size() - 1);
    } else {
      lastStep = null;
    }
  }

  public OResult toResult(OToResultContext ctx) {
    OResultInternal result = new OResultInternal();
    result.setProperty("type", "QueryExecutionPlan");
    result.setProperty(JAVA_TYPE, getClass().getName());
    result.setProperty("cost", getCost());
    result.setProperty("prettyPrint", prettyPrint(new OPrintContexImpl(ctx.getContext(), 0, 2)));
    result.setProperty("stmText", getStatement());
    result.setProperty("genericStm", getGenericStatement());
    result.setProperty(
        "steps",
        steps == null
            ? null
            : steps.stream().map(x -> x.toResult(ctx)).collect(Collectors.toList()));
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
    result.setProperty("prettyPrint", prettyPrint(new OPrintContexImpl(null, 0, 2)));
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
            (OExecutionStepInternal)
                Class.forName(className).getDeclaredConstructor().newInstance();
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
    OSelectExecutionPlan copy = new OSelectExecutionPlan();
    copyOn(copy, ctx);
    return copy;
  }

  protected void copyOn(OSelectExecutionPlan copy, OCommandContext ctx) {
    OExecutionStepInternal lastStep = null;
    for (OExecutionStepInternal step : this.steps) {
      OExecutionStepInternal newStep =
          (OExecutionStepInternal) ((OExecutionStepInternal) step).copy(ctx);
      newStep.setPrevious((OExecutionStepInternal) lastStep);
      lastStep = newStep;
      copy.getSteps().add(newStep);
    }
    copy.lastStep = copy.steps.size() == 0 ? null : copy.steps.get(copy.steps.size() - 1);
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

  @Override
  public Set<String> getIndexes() {
    Set<String> indexes = new HashSet<>();
    for (OExecutionStepInternal chilStep : steps) {
      OExecutionStepInternal.fillIndexes(chilStep, indexes);
    }
    return indexes;
  }
}
