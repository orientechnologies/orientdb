package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OGroupBy;
import com.orientechnologies.orient.core.sql.parser.OProjection;
import com.orientechnologies.orient.core.sql.parser.OProjectionItem;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Created by luigidellaquila on 12/07/16. */
public class AggregateProjectionCalculationStep extends ProjectionCalculationStep {

  private final OGroupBy groupBy;
  private final long timeoutMillis;
  private final long limit;

  public AggregateProjectionCalculationStep(
      OProjection projection, OGroupBy groupBy, long limit, long timeoutMillis) {
    super(projection);
    this.groupBy = groupBy;
    this.timeoutMillis = timeoutMillis;
    this.limit = limit;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    return executeAggregation(ctx);
  }

  private OExecutionStream executeAggregation(OCommandContext ctx) {
    long timeoutBegin = System.nanoTime() / 1_000_000;
    if (!prev.isPresent()) {
      throw new OCommandExecutionException(
          "Cannot execute an aggregation or a GROUP BY without a previous result");
    }
    OExecutionStepInternal prevStep = prev.get();
    OExecutionStream lastRs = prevStep.start(ctx);
    if (timeoutMillis > 0) {
      lastRs = lastRs.timeout(timeoutMillis, this::fail);
    }
    Map<List, OResultInternal> aggregateResults = new LinkedHashMap<>();
    while (lastRs.hasNext(ctx)) {
      aggregate(lastRs.next(ctx), ctx, aggregateResults);
    }
    lastRs.close(ctx);
    OExecutionStream stream =
        OExecutionStream.resultCollection((Collection) aggregateResults.values());
    stream =
        stream.map(
            (res, cont) -> {
              OResultInternal item = (OResultInternal) res;
              for (String name : item.getTemporaryProperties()) {
                Object prevVal = item.getTemporaryProperty(name);
                if (prevVal instanceof AggregationContext) {
                  item.setTemporaryProperty(
                      name, ((AggregationContext) prevVal).getFinalValue(ctx));
                }
              }
              return item;
            });
    if (timeoutMillis > 0) {
      long currentTime = System.nanoTime() / 1_000_000;
      long usedTime = currentTime - timeoutBegin;
      stream = stream.timeout(timeoutMillis - usedTime, this::fail);
    }
    return stream;
  }

  private void fail() {
    throw new OTimeoutException("Timeout expired");
  }

  private void aggregate(
      OResult next, OCommandContext ctx, Map<List, OResultInternal> aggregateResults) {
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
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    String spaces = OExecutionStepInternal.getIndent(ctx);
    String result = spaces + "+ CALCULATE AGGREGATE PROJECTIONS";
    if (ctx.isProfilingEnabled()) {
      result += " (" + ctx.getCostFormatted(this) + ")";
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
  public OExecutionStepInternal copy(OCommandContext ctx) {
    return new AggregateProjectionCalculationStep(
        projection.copy(), groupBy == null ? null : groupBy.copy(), limit, timeoutMillis);
  }
}
