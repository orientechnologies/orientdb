package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.db.OExecutionThreadLocal;
import com.orientechnologies.orient.core.exception.OCommandInterruptedException;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Map;
import java.util.Optional;

public class OInterruptResultSet implements OResultSet {

  private OResultSet source;

  public OInterruptResultSet(OResultSet source) {
    this.source = source;
  }

  @Override
  public boolean hasNext() {
    if (OExecutionThreadLocal.isInterruptCurrentOperation()) {
      throw new OCommandInterruptedException("The command has been interrupted");
    }
    return source.hasNext();
  }

  @Override
  public OResult next() {
    if (OExecutionThreadLocal.isInterruptCurrentOperation()) {
      throw new OCommandInterruptedException("The command has been interrupted");
    }
    return source.next();
  }

  @Override
  public void close() {
    source.close();
  }

  @Override
  public Optional<OExecutionPlan> getExecutionPlan() {
    return source.getExecutionPlan();
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return source.getQueryStats();
  }
}
