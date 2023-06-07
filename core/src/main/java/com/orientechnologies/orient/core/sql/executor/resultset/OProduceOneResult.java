package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Map;
import java.util.Optional;

public class OProduceOneResult implements OResultSet {

  public interface OProduceResult {
    OResult produce();
  }

  private boolean executed = false;
  private final OProduceResult producer;
  private final boolean resettable;

  public OProduceOneResult(OProduceResult producer) {
    if (producer == null) {
      throw new NullPointerException();
    }
    this.producer = producer;
    resettable = false;
  }

  public OProduceOneResult(OProduceResult producer, boolean resettable) {
    if (producer == null) {
      throw new NullPointerException();
    }
    this.producer = producer;
    this.resettable = resettable;
  }

  @Override
  public boolean hasNext() {
    return !executed;
  }

  @Override
  public OResult next() {
    if (executed) {
      throw new IllegalStateException();
    }
    OResult result = producer.produce();
    executed = true;
    return result;
  }

  @Override
  public void close() {}

  @Override
  public void reset() {
    if (resettable) {
      executed = false;
    }
  }

  @Override
  public Optional<OExecutionPlan> getExecutionPlan() {
    return Optional.empty();
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return null;
  }
}
