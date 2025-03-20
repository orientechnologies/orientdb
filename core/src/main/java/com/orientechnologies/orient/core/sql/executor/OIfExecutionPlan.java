package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OIfExecutionPlan implements OInternalExecutionPlan {

  protected IfStep step;
  private String genericStatement;
  private String statement;

  public OIfExecutionPlan() {}

  @Override
  public OExecutionStream start(OCommandContext ctx) {
    return step.start(ctx);
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    StringBuilder result = new StringBuilder();
    result.append(step.prettyPrint(ctx));
    return result.toString();
  }

  public void chain(IfStep step) {
    this.step = step;
  }

  @Override
  public List<OExecutionStepInternal> getSteps() {
    // TODO do a copy of the steps
    return Collections.singletonList(step);
  }

  public void setSteps(List<OExecutionStepInternal> steps) {
    this.step = (IfStep) steps.get(0);
  }

  @Override
  public OResult toResult(OToResultContext ctx) {
    OResultInternal result = new OResultInternal();
    result.setProperty("type", "IfExecutionPlan");
    result.setProperty("javaType", getClass().getName());
    result.setProperty("cost", getCost());
    result.setProperty("prettyPrint", prettyPrint(new OPrintContexImpl(ctx.getContext(), 0, 2)));
    result.setProperty("stmText", getStatement());
    result.setProperty("genericStm", getGenericStatement());
    result.setProperty("steps", Collections.singletonList(step.toResult(ctx)));
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

  @Override
  public Set<String> getIndexes() {
    Set<String> indexes = new HashSet<>();
    OExecutionStepInternal.fillIndexes(step, indexes);
    return indexes;
  }

  @Override
  public void setGenericStatement(String stm) {
    this.genericStatement = stm;
  }

  @Override
  public String getGenericStatement() {
    return this.genericStatement;
  }

  @Override
  public void setStatement(String stm) {
    this.statement = stm;
  }

  @Override
  public String getStatement() {
    return statement;
  }
}
