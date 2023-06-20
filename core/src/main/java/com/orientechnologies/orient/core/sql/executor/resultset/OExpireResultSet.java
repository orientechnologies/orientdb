package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;

public final class OExpireResultSet implements OExecutionStream {
  private final TimedOut timedout;
  private final OExecutionStream internal;
  protected boolean timedOut = false;
  private long expiryTime;

  public interface TimedOut {
    void timeout();
  }

  public OExpireResultSet(OExecutionStream internal, long timeoutMillis, TimedOut timedout) {
    this.internal = internal;
    this.timedout = timedout;
    this.expiryTime = System.currentTimeMillis() + timeoutMillis;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    if (System.currentTimeMillis() > expiryTime) {
      fail();
    }
    if (timedOut) {
      return false;
    }
    return internal.hasNext(ctx);
  }

  @Override
  public OResult next(OCommandContext ctx) {
    if (System.currentTimeMillis() > expiryTime) {
      fail();
      if (timedOut) {
        return new OResultInternal();
      }
    }
    return internal.next(ctx);
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
