package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OResult;

public final class OMultipleExecutionStream implements OExecutionStream {
  private final OExecutionStreamProducer streamsSource;
  private OExecutionStream currentStream;

  public OMultipleExecutionStream(OExecutionStreamProducer streamSource) {
    this.streamsSource = streamSource;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    while (currentStream == null || !currentStream.hasNext(ctx)) {
      if (currentStream != null) {
        currentStream.close(ctx);
      }
      if (!streamsSource.hasNext(ctx)) {
        return false;
      }
      currentStream = streamsSource.next(ctx);
    }
    return true;
  }

  @Override
  public OResult next(OCommandContext ctx) {
    if (!hasNext(ctx)) {
      throw new IllegalStateException();
    }
    return currentStream.next(ctx);
  }

  @Override
  public void close(OCommandContext ctx) {
    if (currentStream != null) {
      currentStream.close(ctx);
    }
    streamsSource.close(ctx);
  }
}
