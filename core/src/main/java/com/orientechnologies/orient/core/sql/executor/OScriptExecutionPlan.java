/** Created by luigidellaquila on 08/08/16. */
package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OScriptExecutionPlan implements OInternalExecutionPlan {

  private boolean executed = false;
  protected List<ScriptLineStep> steps = new ArrayList<>();
  private OExecutionStepInternal lastStep = null;
  private OExecutionStream finalResult = null;
  private String statement;
  private String genericStatement;

  public OScriptExecutionPlan() {}

  @Override
  public void reset(OCommandContext ctx) {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    lastStep.close();
  }

  @Override
  public OExecutionStream start(OCommandContext ctx) {
    doExecute(ctx);
    return finalResult;
  }

  private void doExecute(OCommandContext ctx) {
    if (!executed) {
      executeUntilReturn(ctx);
      executed = true;
      List<OResult> collected = new ArrayList<>();
      OExecutionStream results = lastStep.start(ctx);
      while (results.hasNext(ctx)) {
        collected.add(results.next(ctx));
      }
      results.close(ctx);
      if (lastStep instanceof ScriptLineStep) {
        // collected.setPlan(((ScriptLineStep) lastStep).plan);
      }
      finalResult = OExecutionStream.resultIterator(collected.iterator());
    }
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

  public void chain(
      OInternalExecutionPlan nextPlan, boolean profilingEnabled, OCommandContext ctx) {
    ScriptLineStep lastStep = steps.size() == 0 ? null : steps.get(steps.size() - 1);
    ScriptLineStep nextStep = new ScriptLineStep(nextPlan, ctx, profilingEnabled);
    if (lastStep != null) {
      lastStep.setNext(nextStep);
      nextStep.setPrevious(lastStep);
    }
    steps.add(nextStep);
    this.lastStep = nextStep;
  }

  @Override
  public List<OExecutionStep> getSteps() {
    // TODO do a copy of the steps
    return (List) steps;
  }

  public void setSteps(List<OExecutionStepInternal> steps) {
    this.steps = (List) steps;
  }

  @Override
  public OResult toResult() {
    OResultInternal result = new OResultInternal();
    result.setProperty("type", "ScriptExecutionPlan");
    result.setProperty("javaType", getClass().getName());
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

  @Override
  public boolean canBeCached() {
    return false;
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

  /**
   * executes all the script and returns last statement execution step, so that it can be executed
   * from outside
   * @param ctx TODO
   *
   * @return
   */
  public OExecutionStepInternal executeUntilReturn(OCommandContext ctx) {
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
      OExecutionStream lastResult = step.start(ctx);

      while (lastResult.hasNext(ctx)) {
        lastResult.next(ctx);
      }
      lastResult.close(ctx);
    }
    this.lastStep = steps.get(steps.size() - 1);
    return lastStep;
  }

  /**
   * executes the whole script and returns last statement ONLY if it's a RETURN, otherwise it
   * returns null;
   * @param ctx TODO
   *
   * @return
   */
  public OExecutionStepInternal executeFull(OCommandContext ctx) {
    for (int i = 0; i < steps.size(); i++) {
      ScriptLineStep step = steps.get(i);
      if (step.containsReturn()) {
        OExecutionStepInternal returnStep = step.executeUntilReturn(ctx);
        if (returnStep != null) {
          return returnStep;
        }
      }
      OExecutionStream lastResult = step.start(ctx);

      while (lastResult.hasNext(ctx)) {
        lastResult.next(ctx);
      }
      lastResult.close(ctx);
    }

    return null;
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
    for (OExecutionStep chilStep : steps) {
      OExecutionStepInternal.fillIndexes(chilStep, indexes);
    }
    return indexes;
  }
}
