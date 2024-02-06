package com.orientechnologies.orient.core.sql.executor;

/** Created by luigidellaquila on 08/08/16. */
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.Collections;
import java.util.List;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OIfExecutionPlan implements OInternalExecutionPlan {

  private String location;

  private final OCommandContext ctx;

  @Override
  public OCommandContext getContext() {
    return ctx;
  }

  protected IfStep step;

  public OIfExecutionPlan(OCommandContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public void reset(OCommandContext ctx) {
    // TODO
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    step.close();
  }

  @Override
  public OExecutionStream start() {
    return step.start(ctx);
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    StringBuilder result = new StringBuilder();
    result.append(step.prettyPrint(depth, indent));
    return result.toString();
  }

  public void chain(IfStep step) {
    this.step = step;
  }

  @Override
  public List<OExecutionStep> getSteps() {
    // TODO do a copy of the steps
    return Collections.singletonList(step);
  }

  public void setSteps(List<OExecutionStepInternal> steps) {
    this.step = (IfStep) steps.get(0);
  }

  @Override
  public OResult toResult() {
    OResultInternal result = new OResultInternal();
    result.setProperty("type", "IfExecutionPlan");
    result.setProperty("javaType", getClass().getName());
    result.setProperty("cost", getCost());
    result.setProperty("prettyPrint", prettyPrint(0, 2));
    result.setProperty("steps", Collections.singletonList(step.toResult()));
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

  public OExecutionStepInternal executeUntilReturn() {
    OScriptExecutionPlan plan = step.producePlan(ctx);
    if (plan != null) {
      return plan.executeUntilReturn();
    } else {
      return null;
    }
  }

  public boolean containsReturn() {
    return step.containsReturn();
  }
}
