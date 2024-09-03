package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSetInternal;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

public class OExecutionResultSet implements OResultSetInternal {

  private final OExecutionStream stream;
  private final OCommandContext context;
  private final Optional<OExecutionPlan> plan;
  private boolean closed = false;

  public OExecutionResultSet(
      OExecutionStream stream, OCommandContext context, OExecutionPlan plan) {
    super();
    this.stream = stream;
    this.context = context;
    this.plan = Optional.ofNullable(plan);
  }

  @Override
  public boolean hasNext() {
    if (closed) {
      return false;
    } else {
      return stream.hasNext(context);
    }
  }

  @Override
  public OResult next() {
    if (closed) {
      throw new NoSuchElementException();
    } else {
      return stream.next(context);
    }
  }

  @Override
  public void close() {
    if (!closed) {
      stream.close(context);
      closed = true;
    }
  }

  @Override
  public Optional<OExecutionPlan> getExecutionPlan() {
    return plan;
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return null;
  }

  @Override
  public boolean isDetached() {
    return false;
  }

  @Override
  public boolean isExplain() {
    return false;
  }
}
