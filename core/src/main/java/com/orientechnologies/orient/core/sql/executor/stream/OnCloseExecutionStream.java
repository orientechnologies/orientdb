package com.orientechnologies.orient.core.sql.executor.stream;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OResult;

public class OnCloseExecutionStream implements OExecutionStream {

  private OExecutionStream source;
  private OnClose onClose;

  public OnCloseExecutionStream(OExecutionStream source, OnClose onClose) {
    this.source = source;
    this.onClose = onClose;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    return source.hasNext(ctx);
  }

  @Override
  public OResult next(OCommandContext ctx) {
    return source.next(ctx);
  }

  @Override
  public void close(OCommandContext ctx) {
    onClose.close(ctx);
    source.close(ctx);
  }

  @Override
  public boolean isFullInMemory(OCommandContext ctx) {
    return source.isFullInMemory(ctx);
  }

  @Override
  public boolean isTermination(OCommandContext ctx) {
    return source.isTermination(ctx);
  }
}
