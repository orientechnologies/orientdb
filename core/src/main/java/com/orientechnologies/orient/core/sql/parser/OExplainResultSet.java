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
public class OExplainResultSet implements OTodoResultSet{
  private final OExecutionPlan executionPlan;

  public OExplainResultSet(OExecutionPlan executionPlan) {
    this. executionPlan = executionPlan;
  }

  @Override public boolean hasNext() {
    return false;
  }

  @Override public OResult next() {
    return null;
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
