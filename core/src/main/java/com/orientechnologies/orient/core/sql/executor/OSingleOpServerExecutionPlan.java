package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OBasicServerCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OServerCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.parser.OSimpleExecServerStatement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OSingleOpServerExecutionPlan implements OInternalExecutionPlan {

  protected final OSimpleExecServerStatement statement;
  private OServerCommandContext ctx;

  private boolean executed = false;
  private OResultSet result;

  public OSingleOpServerExecutionPlan(OServerCommandContext ctx, OSimpleExecServerStatement stm) {
    this.ctx = ctx;
    this.statement = stm;
  }

  @Override
  public void close() {}

  @Override
  public OResultSet fetchNext(int n) {
    if (executed && result == null) {
      return new OInternalResultSet();
    }
    if (!executed) {
      executed = true;
      result = statement.executeSimple(this.ctx);
      if (result instanceof OInternalResultSet) {
        ((OInternalResultSet) result).plan = this;
      }
    }
    return new OResultSet() {
      private int fetched = 0;

      @Override
      public boolean hasNext() {
        return fetched < n && result.hasNext();
      }

      @Override
      public OResult next() {
        if (fetched >= n) {
          throw new IllegalStateException();
        }
        fetched++;
        return result.next();
      }

      @Override
      public void close() {
        result.close();
      }

      @Override
      public Optional<OExecutionPlan> getExecutionPlan() {
        return Optional.empty();
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };
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

  public OResultSet executeInternal(OBasicServerCommandContext ctx)
      throws OCommandExecutionException {
    if (executed) {
      throw new OCommandExecutionException(
          "Trying to execute a result-set twice. Please use reset()");
    }
    executed = true;
    result = statement.executeSimple(this.ctx);
    if (result instanceof OInternalResultSet) {
      ((OInternalResultSet) result).plan = this;
    }
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
