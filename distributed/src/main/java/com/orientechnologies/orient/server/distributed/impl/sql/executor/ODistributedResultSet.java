package com.orientechnologies.orient.server.distributed.impl.sql.executor;

import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 21/06/17.
 */
public class ODistributedResultSet implements OResultSet {
  @Override
  public boolean hasNext() {
    throw new UnsupportedOperationException();
  }

  @Override
  public OResult next() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<OExecutionPlan> getExecutionPlan() {
    return null;
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return null;
  }
}
