package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;

/** Created by luigidellaquila on 03/08/16. */
public class GlobalLetExpressionStep extends AbstractExecutionStep {
  private final OIdentifier varname;
  private final OExpression expression;

  private boolean executed = false;

  public GlobalLetExpressionStep(
      OIdentifier varName, OExpression expression, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.varname = varName;
    this.expression = expression;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    calculate(ctx);
    return new OInternalResultSet();
  }

  private void calculate(OCommandContext ctx) {
    if (executed) {
      return;
    }
    Object value = expression.execute((OResult) null, ctx);
    ctx.setVariable(varname.getStringValue(), value);
    executed = true;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ LET (once)\n" + spaces + "  " + varname + " = " + expression;
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new GlobalLetExpressionStep(varname.copy(), expression.copy(), ctx, profilingEnabled);
  }

  @Override
  public boolean canBeCached() {
    return true;
  }
}
