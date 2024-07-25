package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import java.util.Iterator;

public final class OExcutionStreamProducerFromIterator<T> implements OExecutionStreamProducer {
  private final OExecutionStreamProducerValueMap<T> map;
  private Iterator<T> source;

  public OExcutionStreamProducerFromIterator(
      Iterator<T> producer, OExecutionStreamProducerValueMap<T> map) {
    this.map = map;
    this.source = producer;
  }

  @Override
  public OExecutionStream next(OCommandContext ctx) {
    return map.map(source.next(), ctx);
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    return source.hasNext();
  }

  @Override
  public void close(OCommandContext ctx) {}
}
