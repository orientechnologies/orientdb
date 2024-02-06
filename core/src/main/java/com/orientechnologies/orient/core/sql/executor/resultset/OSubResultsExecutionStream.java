package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.Iterator;

public final class OSubResultsExecutionStream implements OExecutionStream {
  private final Iterator<OExecutionStream> streamsSource;
  private OExecutionStream currentStream;

  public OSubResultsExecutionStream(Iterator<OExecutionStream> streamSource) {
    this.streamsSource = streamSource;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    while (currentStream == null || !currentStream.hasNext(ctx)) {
      if (currentStream != null) {
        currentStream.close(ctx);
      }
      if (!streamsSource.hasNext()) {
        return false;
      }
      currentStream = streamsSource.next();
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
    while (streamsSource.hasNext()) {
      streamsSource.next().close(ctx);
    }
  }
}
