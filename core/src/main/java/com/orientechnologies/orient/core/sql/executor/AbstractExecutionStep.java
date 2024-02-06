package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OStepStats;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.text.DecimalFormat;
import java.util.Optional;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public abstract class AbstractExecutionStep implements OExecutionStepInternal {

  protected final OCommandContext ctx;
  protected Optional<OExecutionStepInternal> prev = Optional.empty();
  protected Optional<OExecutionStepInternal> next = Optional.empty();
  protected boolean profilingEnabled = false;

  public AbstractExecutionStep(OCommandContext ctx, boolean profilingEnabled) {
    this.ctx = ctx;
    this.profilingEnabled = profilingEnabled;
  }

  @Override
  public void setPrevious(OExecutionStepInternal step) {
    this.prev = Optional.ofNullable(step);
  }

  @Override
  public void setNext(OExecutionStepInternal step) {
    this.next = Optional.ofNullable(step);
  }

  public OCommandContext getContext() {
    return ctx;
  }

  public Optional<OExecutionStepInternal> getPrev() {
    return prev;
  }

  public Optional<OExecutionStepInternal> getNext() {
    return next;
  }

  @Override
  public void sendTimeout() {
    prev.ifPresent(p -> p.sendTimeout());
  }

  private boolean alreadyClosed = false;
  private long baseCost = 0;

  @Override
  public void close() {
    if (alreadyClosed) {
      return;
    }
    alreadyClosed = true;
    prev.ifPresent(p -> p.close());
  }

  public boolean isProfilingEnabled() {
    return profilingEnabled;
  }

  public void setProfilingEnabled(boolean profilingEnabled) {
    this.profilingEnabled = profilingEnabled;
  }

  public OExecutionStream start(OCommandContext ctx) throws OTimeoutException {
    if (profilingEnabled) {
      ctx.startProfiling(this);
      try {
        return internalStart(ctx).profile(this);
      } finally {
        ctx.endProfiling(this);
      }
    } else {
      return internalStart(ctx);
    }
  }
  ;

  protected abstract OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException;

  @Override
  public long getCost() {
    OStepStats stats = this.ctx.getStats(this);
    if (stats != null) {
      return stats.getCost();
    } else {
      return OExecutionStepInternal.super.getCost();
    }
  }

  protected String getCostFormatted() {
    return new DecimalFormat().format(getCost() / 1000) + "μs";
  }
}
