package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Map;
import java.util.Optional;

public class OCostMeasureResultSet implements OResultSet {

  private OResultSet set;
  private long cost;

  public OCostMeasureResultSet(OResultSet set, long base) {
    this.set = set;
    this.cost = base;
  }

  @Override
  public boolean hasNext() {
    long begin = System.nanoTime();
    try {
      return set.hasNext();
    } finally {
      cost += (System.nanoTime() - begin);
    }
  }

  @Override
  public OResult next() {
    long begin = System.nanoTime();
    try {
      return set.next();
    } finally {
      cost += (System.nanoTime() - begin);
    }
  }

  @Override
  public void close() {
    set.close();
  }

  @Override
  public Optional<OExecutionPlan> getExecutionPlan() {
    return set.getExecutionPlan();
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return set.getQueryStats();
  }

  public long getCost() {
    return cost;
  }
}
