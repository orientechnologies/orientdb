package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OTodoResultSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 07/07/16.
 */
public class OExternalResultSet implements OTodoResultSet {
  private final OExecutionPlan executionPlan;

  private OTodoResultSet lastFetch = null;
  private boolean        finished  = false;

  public OExternalResultSet(OExecutionPlan executionPlan) {
    this.executionPlan = executionPlan;
    fetchNext();
  }

  private boolean fetchNext() {
    lastFetch = executionPlan.fetchNext(100);
    if (!lastFetch.hasNext()) {
      finished = true;
      return false;
    }
    return true;
  }

  @Override public boolean hasNext() {
    if (finished) {
      return false;
    }
    if (lastFetch.hasNext()) {
      return true;
    } else {
      return fetchNext();
    }
  }

  @Override public OResult next() {
    return lastFetch.next();
  }

  @Override public void close() {
    executionPlan.close();
  }

  @Override public Optional<OExecutionPlan> getExecutionPlan() {
    return Optional.empty();//TODO
  }

  @Override public Map<String, Object> getQueryStats() {
    return new HashMap<>();//TODO
  }
}
