package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OSimpleExecStatement;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OSingleOpExecutionPlan implements OInternalExecutionPlan {

  protected final OSimpleExecStatement statement;
  private String genericStatement;
  private String stringStatement;

  public OSingleOpExecutionPlan(OSimpleExecStatement stm) {
    this.statement = stm;
  }

  @Override
  public OExecutionStream start(OCommandContext ctx) {
    return statement.executeSimple(ctx);
  }

  @Override
  public long getCost() {
    return 0;
  }

  @Override
  public boolean canBeCached() {
    return false;
  }

  @Override
  public List<OExecutionStepInternal> getSteps() {
    return Collections.emptyList();
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    String spaces = OExecutionStepInternal.getIndent(ctx);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ ");
    result.append(statement.prettyPrint(ctx));
    return result.toString();
  }

  @Override
  public OResult toResult(OToResultContext ctx) {
    OResultInternal result = new OResultInternal();
    result.setProperty("type", "QueryExecutionPlan");
    result.setProperty("javaType", getClass().getName());
    result.setProperty("stmText", statement.toString());
    result.setProperty("genericStm", getGenericStatement());
    result.setProperty("cost", getCost());
    result.setProperty("prettyPrint", prettyPrint(new OPrintContexImpl(ctx.getContext(), 0, 2)));
    result.setProperty("steps", null);
    return result;
  }

  @Override
  public Set<String> getIndexes() {
    return Collections.emptySet();
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
    this.stringStatement = stm;
  }

  @Override
  public String getStatement() {
    return stringStatement;
  }
}
