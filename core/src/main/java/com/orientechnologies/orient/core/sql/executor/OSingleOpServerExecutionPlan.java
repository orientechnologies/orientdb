package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OBasicServerCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OServerCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OSimpleExecServerStatement;
import java.util.Collections;
import java.util.List;

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

  @Override
  public List<OExecutionStep> getSteps() {
    return Collections.emptyList();
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ ");
    result.append(statement.toString());
    return result.toString();
  }

  @Override
  public OResult toResult() {
    OResultInternal result = new OResultInternal();
    result.setProperty("type", "QueryExecutionPlan");
    result.setProperty("javaType", getClass().getName());
    result.setProperty("stmText", statement.toString());
    result.setProperty("cost", getCost());
    result.setProperty("prettyPrint", prettyPrint(0, 2));
    result.setProperty("steps", null);
    return result;
  }

  @Override
  public void close() {}
}
