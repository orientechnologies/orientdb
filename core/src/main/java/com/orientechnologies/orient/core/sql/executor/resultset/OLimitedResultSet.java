package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Map;
import java.util.Optional;

public class OLimitedResultSet implements OResultSet {
  private final OResultSet upstream;
  private final int limit;
  private int count = 0;

  public OLimitedResultSet(OResultSet upstream, int limit) {
    this.upstream = upstream;
    this.limit = limit;
  }

  @Override
  public boolean hasNext() {
    if (count >= limit) {
      return false;
    } else {
      return upstream.hasNext();
    }
  }

  @Override
  public OResult next() {
    if (count >= limit) {
      throw new IllegalStateException();
    } else {
      OResult read = upstream.next();
      this.count += 1;
      return read;
    }
  }

  @Override
  public void close() {
    upstream.close();
  }

  @Override
  public Optional<OExecutionPlan> getExecutionPlan() {
    return upstream.getExecutionPlan();
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return upstream.getQueryStats();
  }
}
