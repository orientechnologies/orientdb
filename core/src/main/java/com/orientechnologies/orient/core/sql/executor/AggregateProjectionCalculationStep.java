package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OGroupBy;
import com.orientechnologies.orient.core.sql.parser.OProjection;

/**
 * Created by luigidellaquila on 12/07/16.
 */
public class AggregateProjectionCalculationStep extends ProjectionCalculationStep {

  private final OGroupBy groupBy;

  public AggregateProjectionCalculationStep(OProjection projection, OGroupBy groupBy, OCommandContext ctx) {
    super(projection, ctx);
    this.groupBy = groupBy;
  }

  //TODO!!!

  @Override public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStep.getIndent(depth, indent);
    return spaces + "+ CALCULATE AGGREGATE PROJECTIONS\n" +
        spaces + "      " + projection.toString() + "" +
        (groupBy == null ? "" : (spaces + "      " + groupBy.toString()));
  }
}
