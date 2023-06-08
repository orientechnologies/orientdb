package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.resultset.OResultSetMapper;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import com.orientechnologies.orient.core.sql.parser.OProjectionItem;

/** Created by luigidellaquila on 03/08/16. */
public class LetExpressionStep extends AbstractExecutionStep {
  private OIdentifier varname;
  private OExpression expression;

  public LetExpressionStep(
      OIdentifier varName, OExpression expression, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.varname = varName;
    this.expression = expression;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    if (!getPrev().isPresent()) {
      throw new OCommandExecutionException(
          "Cannot execute a local LET on a query without a target");
    }
    return new OResultSetMapper(getPrev().get().syncPull(ctx), (result) -> mapResult(ctx, result));
  }

  private OResult mapResult(OCommandContext ctx, OResult result) {
    Object value = expression.execute(result, ctx);
    ((OResultInternal) result)
        .setMetadata(varname.getStringValue(), OProjectionItem.convert(value));
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ LET (for each record)\n" + spaces + "  " + varname + " = " + expression;
  }

  @Override
  public OResult serialize() {
    OResultInternal result = OExecutionStepInternal.basicSerialize(this);
    if (varname != null) {
      result.setProperty("varname", varname.serialize());
    }
    if (expression != null) {
      result.setProperty("expression", expression.serialize());
    }
    return result;
  }

  @Override
  public void deserialize(OResult fromResult) {
    try {
      OExecutionStepInternal.basicDeserialize(fromResult, this);
      if (fromResult.getProperty("varname") != null) {
        varname = OIdentifier.deserialize(fromResult.getProperty("varname"));
      }
      if (fromResult.getProperty("expression") != null) {
        expression = new OExpression(-1);
        expression.deserialize(fromResult.getProperty("expression"));
      }
      reset();
    } catch (Exception e) {
      throw OException.wrapException(new OCommandExecutionException(""), e);
    }
  }
}
