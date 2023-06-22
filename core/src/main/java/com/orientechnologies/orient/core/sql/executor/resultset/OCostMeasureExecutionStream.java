package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OExecutionStep;
import com.orientechnologies.orient.core.sql.executor.OResult;

public class OCostMeasureExecutionStream implements OExecutionStream {

  private OExecutionStream set;
  private OExecutionStep step;
  private long cost;

  public OCostMeasureExecutionStream(OExecutionStream set, OExecutionStep step, long base) {
    this.set = set;
    this.cost = base;
    this.step = step;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    long begin = System.nanoTime();
    ctx.startProfiling(this.step);
    try {
      return set.hasNext(ctx);
    } finally {
      ctx.endProfiling(this.step);
      cost += (System.nanoTime() - begin);
    }
  }

  @Override
  public OResult next(OCommandContext ctx) {
    long begin = System.nanoTime();
    ctx.startProfiling(this.step);
    try {
      return set.next(ctx);
    } finally {
      ctx.endProfiling(this.step);
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
