package com.orientechnologies.orient.core.sql.executor.stream;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OResult;

public class OSingletonExecutionStream implements OExecutionStream {

  private boolean executed = false;
  private OResult result;

  public OSingletonExecutionStream(OResult result) {
    this.result = result;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    return !executed;
  }

  @Override
  public OResult next(OCommandContext ctx) {
    if (executed) {
      throw new IllegalStateException();
    }
    executed = true;
    return result;
  }

  @Override
  public void close(OCommandContext ctx) {}

  @Override
  public boolean isTermination(OCommandContext ctx) {
    return false;
  }

  @Override
  public boolean isFullInMemory(OCommandContext ctx) {
    return true;
  }
}
