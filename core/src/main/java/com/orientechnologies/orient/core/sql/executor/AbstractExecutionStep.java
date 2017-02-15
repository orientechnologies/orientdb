package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;

import java.util.Optional;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public abstract class AbstractExecutionStep implements OExecutionStepInternal {

  protected final OCommandContext ctx;
  protected Optional<OExecutionStepInternal> prev     = Optional.empty();
  protected Optional<OExecutionStepInternal> next     = Optional.empty();
  protected boolean                          timedOut = false;

  public AbstractExecutionStep(OCommandContext ctx) {
    this.ctx = ctx;
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
    this.timedOut = true;
    prev.ifPresent(p -> p.sendTimeout());
  }

  @Override
  public void close() {
    prev.ifPresent(p -> p.close());
  }

}
