package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OTodoResultSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 08/07/16.
 */
public class OExplainResultSet implements OTodoResultSet {
  private final OExecutionPlan executionPlan;
  boolean hasNext = true;

  public OExplainResultSet(OExecutionPlan executionPlan) {
    this.executionPlan = executionPlan;
  }

  @Override public boolean hasNext() {
    return hasNext;
  }

  @Override public OResult next() {
    if (!hasNext) {
      throw new IllegalStateException();
    }
    OResult result = new OResult();
    getExecutionPlan().ifPresent(x -> result.setProperty("executionPlan", x.prettyPrint(3)));
    hasNext = false;
    return result;
  }

  @Override public void close() {

  }

  @Override public Optional<OExecutionPlan> getExecutionPlan() {
    return Optional.of(executionPlan);
  }

  @Override public Map<String, Object> getQueryStats() {
    return new HashMap<>();
  }
}
