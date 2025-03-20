package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseStats;
import com.orientechnologies.orient.core.sql.executor.OExecutionStepInternal;
import com.orientechnologies.orient.core.sql.executor.OInternalExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OPrintContexImpl;
import com.orientechnologies.orient.core.sql.executor.OPrintContext;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.OToResultContext;
import com.orientechnologies.orient.core.sql.executor.OToResultContextImpl;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class OProfileExecutionPlan implements OInternalExecutionPlan {

  private OInternalExecutionPlan toProfile;
  private String genericStatement;
  private String statement;

  public OProfileExecutionPlan(OInternalExecutionPlan toProfile) {
    this.toProfile = toProfile;
  }

  @Override
  public List<OExecutionStepInternal> getSteps() {
    return toProfile.getSteps();
  }

  @Override
  public Set<String> getIndexes() {
    return toProfile.getIndexes();
  }

  @Override
  public OExecutionStream start(OCommandContext ctx) {
    ((OBasicCommandContext) ctx).enableProfiling();
    OExecutionStream stream = toProfile.start(ctx);
    OExecutionStream.consume(stream, ctx);
    ODatabaseStats dbStats = ((ODatabaseInternal) ctx.getDatabase()).getStats();
    OResultInternal result = new OResultInternal();
    result.setProperty("executionPlan", toResult(new OToResultContextImpl(ctx)));
    result.setProperty("executionPlanAsString", prettyPrint(new OPrintContexImpl(ctx)));
    result.setProperty("dbStats", dbStats.toResult());
    return OExecutionStream.singleton(result);
  }

  @Override
  public long getCost() {
    return 0;
  }

  @Override
  public boolean canBeCached() {
    return toProfile.canBeCached();
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    StringBuilder result = new StringBuilder();
    result.append("PROFILE \n");
    ctx.incDepth();
    result.append(this.toProfile.prettyPrint(ctx));
    ctx.decDepth();
    return result.toString();
  }

  @Override
  public boolean isExplain() {
    return true;
  }

  @Override
  public OResult toResult(final OToResultContext ctx) {
    OResultInternal result = new OResultInternal();
    result.setProperty("type", "ProfileExecutionPlan");
    result.setProperty(JAVA_TYPE, getClass().getName());
    result.setProperty("cost", getCost());
    result.setProperty("prettyPrint", prettyPrint(new OPrintContexImpl(ctx.getContext())));
    result.setProperty("stmText", getStatement());
    result.setProperty("genericStm", getGenericStatement());
    result.setProperty("statement", Arrays.asList(toProfile.toResult(ctx)));
    List<OExecutionStepInternal> steps = toProfile.getSteps();
    if (steps != null) {
      var resultSteps =
          steps.stream()
              .map(x -> ((OExecutionStepInternal) x).toResult(ctx))
              .collect(Collectors.toList());
      result.setProperty("steps", resultSteps);
    } else {
      result.setProperty("steps", null);
    }
    return result;
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
