package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OResult;

public class OResultSetMapper implements OExecutionStream {
  private final OExecutionStream upstream;
  private final OResultMapper mapper;

  public OResultSetMapper(OExecutionStream upstream, OResultMapper mapper) {
    if (upstream == null || mapper == null) {
      throw new NullPointerException();
    }
    this.upstream = upstream;
    this.mapper = mapper;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    return upstream.hasNext(ctx);
  }

  @Override
  public OResult next(OCommandContext ctx) {
    return this.mapper.map(upstream.next(ctx), ctx);
  }

  @Override
  public void close(OCommandContext ctx) {
    this.upstream.close(ctx);
  }
}
