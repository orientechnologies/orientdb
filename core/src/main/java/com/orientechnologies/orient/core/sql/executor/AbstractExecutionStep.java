package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;

import java.util.Optional;

/**
 * Created by luigidellaquila on 08/07/16.
 */
public abstract class AbstractExecutionStep implements OExecutionStep {

  protected final OCommandContext          ctx;
  protected       Optional<OExecutionStep> prev;
  protected       Optional<OExecutionStep> next;

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
}
