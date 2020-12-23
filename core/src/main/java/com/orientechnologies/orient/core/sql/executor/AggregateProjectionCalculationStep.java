package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OGroupBy;
import com.orientechnologies.orient.core.sql.parser.OProjection;
import com.orientechnologies.orient.core.sql.parser.OProjectionItem;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Created by luigidellaquila on 12/07/16. */
public class AggregateProjectionCalculationStep extends ProjectionCalculationStep {

  private final OGroupBy groupBy;
  private final long timeoutMillis;
  private final long limit;

  // the key is the GROUP BY key, the value is the (partially) aggregated value
  private Map<List, OResultInternal> aggregateResults = new LinkedHashMap<>();
  private List<OResultInternal> finalResults = null;

  private int nextItem = 0;
  private long cost = 0;

  public AggregateProjectionCalculationStep(
      OProjection projection,
      OGroupBy groupBy,
      long limit,
      OCommandContext ctx,
      long timeoutMillis,
      boolean profilingEnabled) {
    super(projection, ctx, profilingEnabled);
    this.groupBy = groupBy;
    this.timeoutMillis = timeoutMillis;
    this.limit = limit;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    if (finalResults == null) {
      executeAggregation(ctx, nRecords);
    }

    return new OResultSet() {
      private int localNext = 0;

      @Override
      public boolean hasNext() {
        if (localNext > nRecords || nextItem >= finalResults.size()) {
          return false;
        }
        return true;
      }

      @Override
      public OResult next() {
        if (localNext > nRecords || nextItem >= finalResults.size()) {
          throw new IllegalStateException();
        }
        OResult result = finalResults.get(nextItem);
        nextItem++;
        localNext++;
        return result;
      }

      @Override
      public void close() {}

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

  private void executeAggregation(OCommandContext ctx, int nRecords) {
    long timeoutBegin = System.currentTimeMillis();
    if (!prev.isPresent()) {
      throw new OCommandExecutionException(
          "Cannot execute an aggregation or a GROUP BY without a previous result");
    }
    OExecutionStepInternal prevStep = prev.get();
    OResultSet lastRs = prevStep.syncPull(ctx, nRecords);
    while (lastRs.hasNext()) {
      if (timeoutMillis > 0 && timeoutBegin + timeoutMillis < System.currentTimeMillis()) {
        sendTimeout();
      }
      aggregate(lastRs.next(), ctx);
      if (!lastRs.hasNext()) {
        lastRs = prevStep.syncPull(ctx, nRecords);
      }
    }
    finalResults = new ArrayList<>();
    finalResults.addAll(aggregateResults.values());
    aggregateResults.clear();
    for (OResultInternal item : finalResults) {
      if (timeoutMillis > 0 && timeoutBegin + timeoutMillis < System.currentTimeMillis()) {
        sendTimeout();
      }
      for (String name : item.getTemporaryProperties()) {
        Object prevVal = item.getTemporaryProperty(name);
        if (prevVal instanceof AggregationContext) {
          item.setTemporaryProperty(name, ((AggregationContext) prevVal).getFinalValue());
        }
      }
    }
  }

  private void aggregate(OResult next, OCommandContext ctx) {
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      List<Object> key = new ArrayList<>();
      if (groupBy != null) {
        for (OExpression item : groupBy.getItems()) {
          Object val = item.execute(next, ctx);
          key.add(val);
        }
      }
      OResultInternal preAggr = aggregateResults.get(key);
      if (preAggr == null) {
        if (limit > 0 && aggregateResults.size() > limit) {
          return;
        }
        preAggr = new OResultInternal();

        for (OProjectionItem proj : this.projection.getItems()) {
          String alias = proj.getProjectionAlias().getStringValue();
          if (!proj.isAggregate()) {
            preAggr.setProperty(alias, proj.execute(next, ctx));
          }
        }
        aggregateResults.put(key, preAggr);
      }

      for (OProjectionItem proj : this.projection.getItems()) {
        String alias = proj.getProjectionAlias().getStringValue();
        if (proj.isAggregate()) {
          AggregationContext aggrCtx = (AggregationContext) preAggr.getTemporaryProperty(alias);
          if (aggrCtx == null) {
            aggrCtx = proj.getAggregationContext(ctx);
            preAggr.setTemporaryProperty(alias, aggrCtx);
          }
          aggrCtx.apply(next, ctx);
        }
      }
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ CALCULATE AGGREGATE PROJECTIONS";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    result +=
        "\n"
            + spaces
            + "      "
            + projection.toString()
            + ""
            + (groupBy == null ? "" : (spaces + "\n  " + groupBy.toString()));
    return result;
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new AggregateProjectionCalculationStep(
        projection.copy(),
        groupBy == null ? null : groupBy.copy(),
        limit,
        ctx,
        timeoutMillis,
        profilingEnabled);
  }

  @Override
  public long getCost() {
    return cost;
  }
}
