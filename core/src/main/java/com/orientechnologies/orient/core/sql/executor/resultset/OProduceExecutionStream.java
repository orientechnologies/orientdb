package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OResult;

public class OProduceExecutionStream implements OExecutionStream {
  private OProduceResult producer;

  public OProduceExecutionStream(OProduceResult producer) {
    if (producer == null) {
      throw new NullPointerException();
    }
    this.producer = producer;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    return true;
  }

  @Override
  public OResult next(OCommandContext ctx) {
    return producer.produce(ctx);
  }

  @Override
  public void close(OCommandContext ctx) {}
}
