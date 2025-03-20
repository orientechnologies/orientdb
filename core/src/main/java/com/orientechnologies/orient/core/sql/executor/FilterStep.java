package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;

/** Created by luigidellaquila on 12/07/16. */
public class FilterStep extends AbstractExecutionStep {
  private final long timeoutMillis;
  private OWhereClause whereClause;
  private final boolean locked;

  public FilterStep(OWhereClause whereClause, long timeoutMillis, boolean locked) {
    super();
    this.whereClause = whereClause;
    this.timeoutMillis = timeoutMillis;
    this.locked = locked;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    if (!prev.isPresent()) {
      throw new IllegalStateException("filter step requires a previous step");
    }

    OExecutionStream resultSet = prev.get().start(ctx);
    resultSet = resultSet.filter(this::filterMap);
    if (timeoutMillis > 0) {
      resultSet = resultSet.timeout(timeoutMillis, this::fail);
    }
    return resultSet;
  }

  private OResult filterMap(OResult result, OCommandContext ctx) {
    if (whereClause.matchesFilters(result, ctx)) {
      return result;
    }
    if (locked && result.getIdentity().isPresent()) {
      ctx.getDatabase().unlock(result.getIdentity().get());
    }
    return null;
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    StringBuilder result = new StringBuilder();
    result.append(OExecutionStepInternal.getIndent(ctx) + "+ FILTER ITEMS WHERE ");
    if (ctx.isProfilingEnabled()) {
      result.append(" (" + ctx.getCostFormatted(this) + ")");
    }
    result.append("\n");
    result.append(OExecutionStepInternal.getIndent(ctx));
    result.append("  ");
    result.append(whereClause.toString());
    return result.toString();
  }

  private void fail() {
    throw new OTimeoutException("Timeout expired");
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
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStepInternal copy(OCommandContext ctx) {
    return new FilterStep(this.whereClause.copy(), timeoutMillis, locked);
  }
}
