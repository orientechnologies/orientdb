package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OCostMeasureResultSet;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.text.DecimalFormat;
import java.util.Optional;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public abstract class AbstractExecutionStep implements OExecutionStepInternal {

  protected final OCommandContext ctx;
  protected Optional<OExecutionStepInternal> prev = Optional.empty();
  protected Optional<OExecutionStepInternal> next = Optional.empty();
  protected boolean profilingEnabled = false;
  private OCostMeasureResultSet costMeasure = null;

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

  protected OExecutionStream attachProfile(OExecutionStream stream) {
    return attachProfile(stream, baseCost);
  }

  protected OExecutionStream attachProfile(OExecutionStream stream, long baseCost) {
    if (profilingEnabled) {
      this.costMeasure = stream.profile(this, baseCost);
      return this.costMeasure;
    } else {
      return stream;
    }
  }

  @Override
  public long getCost() {
    if (costMeasure != null) {
      return costMeasure.getCost();
    } else {
      return OExecutionStepInternal.super.getCost();
    }
  }

  protected interface Measure<T> {
    public T measure(OCommandContext context);
  }

  protected <T> T measure(OCommandContext context, Measure<T> measure) {
    if (profilingEnabled) {
      long begin = System.nanoTime();
      try {
        return measure.measure(context);
      } finally {
        if (profilingEnabled) {
          this.baseCost += (System.nanoTime() - begin);
        }
      }
    } else {
      return measure.measure(context);
    }
  }

  protected String getCostFormatted() {
    return new DecimalFormat().format(getCost() / 1000) + "μs";
  }
}
