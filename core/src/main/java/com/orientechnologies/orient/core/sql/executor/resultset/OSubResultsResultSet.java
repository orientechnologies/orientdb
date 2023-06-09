package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

public final class OSubResultsResultSet implements OResultSet {
  private final Iterator<OResultSet> subSteps;
  private OResultSet currentResultSet;

  public OSubResultsResultSet(Iterator<OResultSet> subSteps) {
    this.subSteps = subSteps;
  }

  @Override
  public boolean hasNext() {
    while (currentResultSet == null || !currentResultSet.hasNext()) {
      if (currentResultSet != null) {
        currentResultSet.close();
      }
      if (!subSteps.hasNext()) {
        return false;
      }
      currentResultSet = subSteps.next();
    }
    return true;
  }

  @Override
  public OResult next() {
    if (!hasNext()) {
      throw new IllegalStateException();
    }
    return currentResultSet.next();
  }

  @Override
  public void close() {
    if (currentResultSet != null) {
      currentResultSet.close();
    }
    while (subSteps.hasNext()) {
      subSteps.next().close();
    }
  }

  @Override
  public Optional<OExecutionPlan> getExecutionPlan() {
    return Optional.empty();
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return new HashMap<>();
  }
}
