package com.orientechnologies.orient.core.sql.executor.stream;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OResult;

public class OEmptyExecutionStream implements OExecutionStream {

  protected static final OExecutionStream EMPTY = new OEmptyExecutionStream();

  @Override
  public boolean hasNext(OCommandContext ctx) {
    return false;
  }

  @Override
  public OResult next(OCommandContext ctx) {
    throw new IllegalStateException();
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
