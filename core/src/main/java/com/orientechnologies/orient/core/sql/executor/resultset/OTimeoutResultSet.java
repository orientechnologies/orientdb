package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import java.util.concurrent.atomic.AtomicLong;

public final class OTimeoutResultSet implements OExecutionStream {
  private AtomicLong totalTime = new AtomicLong(0);
  private final TimedOut timedout;
  private final OExecutionStream internal;
  private final long timeoutMillis;
  protected boolean timedOut = false;

  public interface TimedOut {
    void timeout();
  }

  public OTimeoutResultSet(OExecutionStream internal, long timeoutMillis, TimedOut timedout) {
    this.internal = internal;
    this.timedout = timedout;
    this.timeoutMillis = timeoutMillis;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    if (timedOut) {
      return false;
    }
    long begin = System.nanoTime();

    try {
      return internal.hasNext(ctx);
    } finally {
      totalTime.addAndGet(System.nanoTime() - begin);
    }
  }

  @Override
  public OResult next(OCommandContext ctx) {
    if (totalTime.get() / 1_000_000 > timeoutMillis) {
      fail();
      if (timedOut) {
        return new OResultInternal();
      }
    }
    long begin = System.nanoTime();
    try {
      return internal.next(ctx);
    } finally {
      totalTime.addAndGet(System.nanoTime() - begin);
    }
  }

  @Override
  public void close(OCommandContext ctx) {
    internal.close(ctx);
  }

  private void fail() {
    this.timedOut = true;
    this.timedout.timeout();
  }
}
