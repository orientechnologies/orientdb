package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Map;
import java.util.Optional;

public final class OExpireResultSet implements OResultSet {
  private final TimedOut timedout;
  private final OResultSet internal;
  protected boolean timedOut = false;
  private long expiryTime;

  public interface TimedOut {
    void timeout();
  }

  public OExpireResultSet(OResultSet internal, long timeoutMillis, TimedOut timedout) {
    this.internal = internal;
    this.timedout = timedout;
    this.expiryTime = System.currentTimeMillis() + timeoutMillis;
  }

  @Override
  public boolean hasNext() {
    if (System.currentTimeMillis() > expiryTime) {
      fail();
    }
    if (timedOut) {
      return false;
    }
    return internal.hasNext();
  }

  @Override
  public OResult next() {
    if (System.currentTimeMillis() > expiryTime) {
      fail();
      if (timedOut) {
        return new OResultInternal();
      }
    }
    return internal.next();
  }

  private void fail() {
    this.timedOut = true;
    this.timedout.timeout();
  }

  @Override
  public void close() {
    internal.close();
  }

  @Override
  public Optional<OExecutionPlan> getExecutionPlan() {
    return internal.getExecutionPlan();
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return internal.getQueryStats();
  }
}
