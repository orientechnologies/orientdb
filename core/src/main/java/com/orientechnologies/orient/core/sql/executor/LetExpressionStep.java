package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;

import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 03/08/16.
 */
public class LetExpressionStep extends AbstractExecutionStep {
  private final OIdentifier varname;
  private final OExpression expression;

  public LetExpressionStep(OIdentifier varName, OExpression expression, OCommandContext ctx) {
    super(ctx);
    this.varname = varName;
    this.expression = expression;
  }

  @Override public OTodoResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    if (!getPrev().isPresent()) {
      throw new OCommandExecutionException("Cannot execute a local LET on a query without a target");
    }
    return new OTodoResultSet() {
      OTodoResultSet source = getPrev().get().syncPull(ctx, nRecords);

      @Override public boolean hasNext() {
        return source.hasNext();
      }

      @Override public OResult next() {
        OResultInternal result = (OResultInternal) source.next();
        Object value = expression.execute(result, ctx);
        result.setProperty(varname.getStringValue(), value);
        return result;
      }

      @Override public void close() {
        source.close();
      }

      @Override public Optional<OExecutionPlan> getExecutionPlan() {
        return null;
      }

      @Override public Map<String, Object> getQueryStats() {
        return null;
      }
    };
  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override public void sendResult(Object o, Status status) {

  }

  @Override public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ LET\n" +
        spaces + "  " + varname + " = " + expression;
  }

}
