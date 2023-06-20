package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OResult;

public class OCostMeasureResultSet implements OExecutionStream {

  private OExecutionStream set;
  private long cost;

  public OCostMeasureResultSet(OExecutionStream set, long base) {
    this.set = set;
    this.cost = base;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    long begin = System.nanoTime();
    try {
      return set.hasNext(ctx);
    } finally {
      cost += (System.nanoTime() - begin);
    }
  }

  @Override
  public OResult next(OCommandContext ctx) {
    long begin = System.nanoTime();
    try {
      return set.next(ctx);
    } finally {
      cost += (System.nanoTime() - begin);
    }
  }

  @Override
  public void close(OCommandContext ctx) {
    set.close(ctx);
  }

  public long getCost() {
    return cost;
  }
}
