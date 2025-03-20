package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OBasicServerCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OServerCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OSimpleExecServerStatement;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OSingleOpServerExecutionPlan implements OServerExecutionPlan {

  protected final OSimpleExecServerStatement statement;

  public OSingleOpServerExecutionPlan(OSimpleExecServerStatement stm) {
    this.statement = stm;
  }

  @Override
  public OExecutionStream start(OServerCommandContext ctx) {
    return statement.executeSimple(ctx);
  }

  public void reset(OCommandContext ctx) {}

  @Override
  public long getCost() {
    return 0;
  }

  @Override
  public boolean canBeCached() {
    return false;
  }

  public OExecutionStream executeInternal(OBasicServerCommandContext ctx)
      throws OCommandExecutionException {
    return statement.executeSimple(ctx);
  }

  public String prettyPrint(OPrintContext ctx) {
    String spaces = OExecutionStepInternal.getIndent(ctx);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ ");
    result.append(statement.toString());
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
  public void close() {}
}
