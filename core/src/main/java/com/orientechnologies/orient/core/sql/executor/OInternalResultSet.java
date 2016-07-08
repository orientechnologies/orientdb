package com.orientechnologies.orient.core.sql.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 07/07/16.
 */
public class OInternalResultSet implements OTodoResultSet {
  List<OResult> content = new ArrayList<>();
  int           next    = 0;

  @Override public boolean hasNext() {
    return content.size() > next;
  }

  @Override public OResult next() {
    return content.get(next++);
  }

  @Override public void close() {
    this.content.clear();
  }

  @Override public Optional<OExecutionPlan> getExecutionPlan() {
    return null;
  }

  @Override public Map<String, Object> getQueryStats() {
    return null;
  }

  public void add(OResult nextResult) {
    content.add(nextResult);
  }

}
