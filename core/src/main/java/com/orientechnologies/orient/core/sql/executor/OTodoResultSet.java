package com.orientechnologies.orient.core.sql.executor;

import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 07/07/16.
 */
public interface OTodoResultSet {

  boolean hasNext();

  OResult next();

  void close();

  Optional<OExecutionPlan> getExecutionPlan();

  Map<String, Object> getQueryStats();

  default void reset() {
    throw new UnsupportedOperationException("Implement RESET on " + getClass().getSimpleName());
  }
}
