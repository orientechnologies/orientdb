package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OResult;

public class OLimitedResultSet implements OExecutionStream {
  private final OExecutionStream upstream;
  private final long limit;
  private long count = 0;

  public OLimitedResultSet(OExecutionStream upstream, long limit) {
    this.upstream = upstream;
    this.limit = limit;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    if (count >= limit) {
      return false;
    } else {
      return upstream.hasNext(ctx);
    }
  }

  @Override
  public OResult next(OCommandContext ctx) {
    if (count >= limit) {
      throw new IllegalStateException();
    } else {
      OResult read = upstream.next(ctx);
      this.count += 1;
      return read;
    }
  }

  @Override
  public void close(OCommandContext ctx) {
    upstream.close(ctx);
  }
}
