package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** Created by luigidellaquila on 13/10/16. */
public class WhileMatchStep extends AbstractUnrollStep {

  private final OInternalExecutionPlan body;
  private final OWhereClause condition;

  public WhileMatchStep(
      OCommandContext ctx,
      OWhereClause condition,
      OInternalExecutionPlan body,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.body = body;
    this.condition = condition;
  }

  @Override
  protected Collection<OResult> unroll(OResult doc, OCommandContext iContext) {
    body.reset(iContext);
    List<OResult> result = new ArrayList<>();
    OResultSet block = body.fetchNext(100);
    while (block.hasNext()) {
      while (block.hasNext()) {
        result.add(block.next());
      }
      block = body.fetchNext(100);
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String indentStep = OExecutionStepInternal.getIndent(1, indent);
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ WHILE\n");

    result.append(spaces);
    result.append(indentStep);
    result.append(condition.toString());
    result.append("\n");

    result.append(spaces);
    result.append("  DO\n");

    result.append(body.prettyPrint(depth + 1, indent));
    result.append("\n");

    result.append(spaces);
    result.append("  END\n");

    return result.toString();
  }
}
