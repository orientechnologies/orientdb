package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;

/** Created by luigidellaquila on 03/08/16. */
public class GlobalLetExpressionStep extends AbstractExecutionStep {
  private final OIdentifier varname;
  private final OExpression expression;

  public GlobalLetExpressionStep(OIdentifier varName, OExpression expression) {
    super();
    this.varname = varName;
    this.expression = expression;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.start(ctx).close(ctx));
    Object value = expression.execute((OResult) null, ctx);
    ctx.setVariable(varname.getStringValue(), value);
    return OExecutionStream.empty();
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    String spaces = OExecutionStepInternal.getIndent(ctx);
    return spaces + "+ LET (once)\n" + spaces + "  " + varname + " = " + expression;
  }

  @Override
  public OExecutionStepInternal copy(OCommandContext ctx) {
    return new GlobalLetExpressionStep(varname.copy(), expression.copy());
  }

  @Override
  public boolean canBeCached() {
    return true;
  }
}
