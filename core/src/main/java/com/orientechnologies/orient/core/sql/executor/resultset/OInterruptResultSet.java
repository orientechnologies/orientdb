package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.OExecutionThreadLocal;
import com.orientechnologies.orient.core.exception.OCommandInterruptedException;
import com.orientechnologies.orient.core.sql.executor.OResult;

public class OInterruptResultSet implements OExecutionStream {

  private OExecutionStream source;

  public OInterruptResultSet(OExecutionStream source) {
    this.source = source;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    if (OExecutionThreadLocal.isInterruptCurrentOperation()) {
      throw new OCommandInterruptedException("The command has been interrupted");
    }
    return source.hasNext(ctx);
  }

  @Override
  public OResult next(OCommandContext ctx) {
    if (OExecutionThreadLocal.isInterruptCurrentOperation()) {
      throw new OCommandInterruptedException("The command has been interrupted");
    }
    return source.next(ctx);
  }

  @Override
  public void close(OCommandContext ctx) {
    source.close(ctx);
  }
}
