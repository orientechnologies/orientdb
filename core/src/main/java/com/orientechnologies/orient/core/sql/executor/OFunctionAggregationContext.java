package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Delegates to an aggregate function for aggregation calculation
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OFunctionAggregationContext implements AggregationContext {
  private OSQLFunction aggregateFunction;
  private List<OExpression> params;
  // SQL-standard DISTINCT per aggregate parameter: dedup values before forwarding to the aggregator.
  private final boolean[] distinctParams;
  private final Set<Object>[] distinctSeen;

  public OFunctionAggregationContext(OSQLFunction function, List<OExpression> params) {
    this(function, params, null);
  }

  @SuppressWarnings("unchecked")
  public OFunctionAggregationContext(
      OSQLFunction function, List<OExpression> params, List<Boolean> distinct) {
    this.aggregateFunction = function;
    this.params = params;
    if (this.params == null) {
      this.params = new ArrayList<>();
    }
    int n = this.params.size();
    this.distinctParams = new boolean[n];
    this.distinctSeen = (Set<Object>[]) new Set<?>[n];
    if (distinct != null) {
      for (int i = 0; i < n && i < distinct.size(); i++) {
        if (Boolean.TRUE.equals(distinct.get(i))) {
          this.distinctParams[i] = true;
          this.distinctSeen[i] = new HashSet<>();
        }
      }
    }
  }

  @Override
  public Object getFinalValue(OCommandContext ctx) {
    return aggregateFunction.getResult(ctx);
  }

  @Override
  public void apply(OResult next, OCommandContext ctx) {
    List<Object> paramValues = new ArrayList<>();
    for (int i = 0; i < params.size(); i++) {
      Object v = params.get(i).execute(next, ctx);
      if (distinctParams[i]) {
        if (v == null) {
          // Standard SQL: DISTINCT inside an aggregate ignores NULLs.
          return;
        }
        if (!distinctSeen[i].add(v)) {
          // Duplicate — skip this row for the aggregator.
          return;
        }
      }
      paramValues.add(v);
    }
    aggregateFunction.execute(next, null, null, paramValues.toArray(), ctx);
  }
}
