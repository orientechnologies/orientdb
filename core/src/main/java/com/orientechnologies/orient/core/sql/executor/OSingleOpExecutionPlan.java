package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OSimpleExecStatement;
import java.util.Collections;
import java.util.List;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OSingleOpExecutionPlan implements OInternalExecutionPlan {

  protected final OSimpleExecStatement statement;

  private boolean executed = false;
  private OExecutionStream result;

  public OSingleOpExecutionPlan(OSimpleExecStatement stm) {
    this.statement = stm;
  }

  @Override
  public void close() {}

  @Override
  public OExecutionStream start(OCommandContext ctx) {
    if (executed && result == null) {
      return OExecutionStream.empty();
    }
    if (!executed) {
      executed = true;
      result = statement.executeSimple(ctx);
    }
    return result;
  }

  public void reset(OCommandContext ctx) {
    executed = false;
  }

  @Override
  public long getCost() {
    return 0;
  }

  @Override
  public boolean canBeCached() {
    return false;
  }

  public OExecutionStream executeInternal(OBasicCommandContext ctx)
      throws OCommandExecutionException {
    if (executed) {
      throw new OCommandExecutionException(
          "Trying to execute a result-set twice. Please use reset()");
    }
    executed = true;
    result = statement.executeSimple(ctx);
    return result;
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
}
