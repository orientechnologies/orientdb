package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OResult;

public class OProduceResultSet implements OExecutionStream {
  public interface OProduceResult {
    OResult produce();
  }

  private OProduceResult producer;

  public OProduceResultSet(OProduceResult producer) {
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
    return producer.produce();
  }

  @Override
  public void close(OCommandContext ctx) {}
}
