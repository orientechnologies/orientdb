package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Map;
import java.util.Optional;

public class OProduceResultSet implements OResultSet {
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
  public boolean hasNext() {
    return true;
  }

  @Override
  public OResult next() {
    return producer.produce();
  }

  @Override
  public void close() {}

  @Override
  public Optional<OExecutionPlan> getExecutionPlan() {
    return Optional.empty();
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return null;
  }
}
