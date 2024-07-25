package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;

public interface OExecutionStreamProducerValueMap<T> {
  OExecutionStream map(T value, OCommandContext ctx);
}
