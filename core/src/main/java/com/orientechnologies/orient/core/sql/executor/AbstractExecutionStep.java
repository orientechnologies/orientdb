package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;

import java.util.Optional;

/**
 * @author Luigi Dell'Aquila
 */
public abstract class AbstractExecutionStep implements OExecutionStep {

  protected final OCommandContext ctx;
  protected Optional<OExecutionStep> prev = Optional.empty();
  protected Optional<OExecutionStep> next = Optional.empty();

  public AbstractExecutionStep(OCommandContext ctx) {
    this.ctx = ctx;
  }

  @Override public void setPrevious(OExecutionStep step) {
    this.prev = Optional.ofNullable(step);
  }

  @Override public void setNext(OExecutionStep step) {
    this.next = Optional.ofNullable(step);
  }

  public OCommandContext getContext() {
    return ctx;
  }

  public Optional<OExecutionStep> getPrev() {
    return prev;
  }

  public Optional<OExecutionStep> getNext() {
    return next;
  }

  @Override public void sendTimeout() {
    prev.ifPresent(p -> p.sendTimeout());
  }

  @Override public void close() {
    prev.ifPresent(p -> p.close());
  }

}
