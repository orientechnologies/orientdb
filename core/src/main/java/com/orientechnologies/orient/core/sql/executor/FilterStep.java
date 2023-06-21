package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.executor.resultset.OExpireResultSet;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;

/** Created by luigidellaquila on 12/07/16. */
public class FilterStep extends AbstractExecutionStep {
  private final long timeoutMillis;
  private OWhereClause whereClause;
  private long cost;

  public FilterStep(
      OWhereClause whereClause, OCommandContext ctx, long timeoutMillis, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.whereClause = whereClause;
    this.timeoutMillis = timeoutMillis;
  }

  @Override
  public OExecutionStream syncPull(OCommandContext ctx) throws OTimeoutException {
    if (!prev.isPresent()) {
      throw new IllegalStateException("filter step requires a previous step");
    }

    OExecutionStream resultSet = prev.get().syncPull(ctx);
    resultSet = resultSet.filter(this::filterMap);
    if (timeoutMillis > 0) {
      resultSet = new OExpireResultSet(resultSet, timeoutMillis, this::sendTimeout);
    }
    return resultSet;
  }

  private OResult filterMap(OResult result, OCommandContext ctx) {
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      if (whereClause.matchesFilters(result, ctx)) {
        return result;
      }
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
    return null;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    StringBuilder result = new StringBuilder();
    result.append(OExecutionStepInternal.getIndent(depth, indent) + "+ FILTER ITEMS WHERE ");
    if (profilingEnabled) {
      result.append(" (" + getCostFormatted() + ")");
    }
    result.append("\n");
    result.append(OExecutionStepInternal.getIndent(depth, indent));
    result.append("  ");
    result.append(whereClause.toString());
    return result.toString();
  }

  @Override
  public OResult serialize() {
    OResultInternal result = OExecutionStepInternal.basicSerialize(this);
    if (whereClause != null) {
      result.setProperty("whereClause", whereClause.serialize());
    }

    return result;
  }

  @Override
  public void deserialize(OResult fromResult) {
    try {
      OExecutionStepInternal.basicDeserialize(fromResult, this);
      whereClause = new OWhereClause(-1);
      whereClause.deserialize(fromResult.getProperty("whereClause"));
    } catch (Exception e) {
      throw OException.wrapException(new OCommandExecutionException(""), e);
    }
  }

  @Override
  public long getCost() {
    return cost;
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new FilterStep(this.whereClause.copy(), ctx, timeoutMillis, profilingEnabled);
  }
}
