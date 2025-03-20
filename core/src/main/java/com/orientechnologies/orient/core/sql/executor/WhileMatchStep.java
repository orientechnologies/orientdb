package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** Created by luigidellaquila on 13/10/16. */
public class WhileMatchStep extends AbstractUnrollStep {

  private final OInternalExecutionPlan body;
  private final OWhereClause condition;

  public WhileMatchStep(OCommandContext ctx, OWhereClause condition, OInternalExecutionPlan body) {
    super();
    this.body = body;
    this.condition = condition;
  }

  @Override
  protected Collection<OResult> unroll(OResult doc, OCommandContext iContext) {
    List<OResult> result = new ArrayList<>();
    OExecutionStream block = body.start(iContext);
    while (block.hasNext(iContext)) {
      result.add(block.next(iContext));
    }
    block.close(iContext);
    return result;
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    String indentStep = OExecutionStepInternal.getIndent(ctx);
    String spaces = OExecutionStepInternal.getIndent(ctx);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ WHILE\n");

    result.append(spaces);
    result.append(indentStep);
    result.append(condition.toString());
    result.append("\n");

    result.append(spaces);
    result.append("  DO\n");

    ctx.incDepth();
    result.append(body.prettyPrint(ctx));
    ctx.decDepth();
    result.append("\n");

    result.append(spaces);
    result.append("  END\n");

    return result.toString();
  }
}
