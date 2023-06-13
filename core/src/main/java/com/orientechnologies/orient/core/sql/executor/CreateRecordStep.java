package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.resultset.OLimitedResultSet;
import com.orientechnologies.orient.core.sql.executor.resultset.OProduceResultSet;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class CreateRecordStep extends AbstractExecutionStep {

  private long cost = 0;
  private int total = 0;

  public CreateRecordStep(OCommandContext ctx, int total, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.total = total;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx));
    return new OLimitedResultSet(new OProduceResultSet(() -> produce(ctx)), total);
  }

  private OResult produce(OCommandContext ctx) {
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      return new OUpdatableResult((ODocument) ctx.getDatabase().newInstance());
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ CREATE EMPTY RECORDS");
    if (profilingEnabled) {
      result.append(" (" + getCostFormatted() + ")");
    }
    result.append("\n");
    result.append(spaces);
    if (total == 1) {
      result.append("  1 record");
    } else {
      result.append("  " + total + " record");
    }
    return result.toString();
  }

  @Override
  public long getCost() {
    return cost;
  }
}
