package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OAdminCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OAdminStatementExecution;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OSingleOpAdminExecutionPlan implements OAdminExecutionPlan {

  protected final OAdminStatementExecution statement;

  public OSingleOpAdminExecutionPlan(OAdminStatementExecution stm) {
    this.statement = stm;
  }

  @Override
  public OExecutionStream start(OAdminCommandContext ctx) {
    return statement.executeSimple(ctx);
  }

  @Override
  public long getCost() {
    return 0;
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
