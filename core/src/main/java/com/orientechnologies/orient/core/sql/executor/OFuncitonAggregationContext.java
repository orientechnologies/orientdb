package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import java.util.ArrayList;
import java.util.List;

/**
 * Delegates to an aggregate function for aggregation calculation
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OFuncitonAggregationContext implements AggregationContext {
  private OSQLFunction aggregateFunction;
  private List<OExpression> params;

  public OFuncitonAggregationContext(OSQLFunction function, List<OExpression> params) {
    this.aggregateFunction = function;
    this.params = params;
    if (this.params == null) {
      this.params = new ArrayList<>();
    }
  }

  @Override
  public Object getFinalValue() {
    return aggregateFunction.getResult();
  }

  @Override
  public void apply(OResult next, OCommandContext ctx) {
    List<Object> paramValues = new ArrayList<>();
    for (OExpression expr : params) {
      paramValues.add(expr.execute(next, ctx));
    }
    aggregateFunction.execute(next, null, null, paramValues.toArray(), ctx);
  }
}
