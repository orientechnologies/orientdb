package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public final class OTimeoutResultSet implements OResultSet {
  private AtomicLong totalTime = new AtomicLong(0);
  private final TimedOut timedout;
  private final OResultSet internal;
  private final long timeoutMillis;
  protected boolean timedOut = false;

  public interface TimedOut {
    void timeout();
  }

  public OTimeoutResultSet(OResultSet internal, long timeoutMillis, TimedOut timedout) {
    this.internal = internal;
    this.timedout = timedout;
    this.timeoutMillis = timeoutMillis;
  }

  @Override
  public boolean hasNext() {
    if (timedOut) {
      return false;
    }
    long begin = System.nanoTime();

    try {
      return internal.hasNext();
    } finally {
      totalTime.addAndGet(System.nanoTime() - begin);
    }
  }

  @Override
  public OResult next() {
    if (totalTime.get() / 1_000_000 > timeoutMillis) {
      fail();
      if (timedOut) {
        return new OResultInternal();
      }
    }
    long begin = System.nanoTime();
    try {
      return internal.next();
    } finally {
      totalTime.addAndGet(System.nanoTime() - begin);
    }
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
