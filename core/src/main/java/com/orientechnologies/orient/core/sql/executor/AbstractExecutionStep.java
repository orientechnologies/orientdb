package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import java.util.Optional;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public abstract class AbstractExecutionStep implements OExecutionStepInternal {

  protected Optional<OExecutionStepInternal> prev = Optional.empty();

  public AbstractExecutionStep() {}

  @Override
  public void setPrevious(OExecutionStepInternal step) {
    this.prev = Optional.ofNullable(step);
  }

  public Optional<OExecutionStepInternal> getPrev() {
    return prev;
  }

  public OExecutionStream start(OCommandContext ctx) throws OTimeoutException {
    if (ctx.isProfiling()) {
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

  protected abstract OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException;
}
