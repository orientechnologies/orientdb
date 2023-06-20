package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.NoSuchElementException;

public class OEmptyExecutionStream implements OExecutionStream {

  @Override
  public boolean hasNext(OCommandContext ctx) {
    return false;
  }

  @Override
  public OResult next(OCommandContext ctx) {
    throw new NoSuchElementException();
  }

  @Override
  public void close(OCommandContext ctx) {}
}
