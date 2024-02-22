package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;

public interface OExecutionStreamProducer {
  boolean hasNext(OCommandContext ctx);

  OExecutionStream next(OCommandContext ctx);

  void close(OCommandContext ctx);
}
