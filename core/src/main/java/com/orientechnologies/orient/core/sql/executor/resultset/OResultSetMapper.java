package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Map;
import java.util.Optional;

public class OResultSetMapper implements OResultSet {
  private final OResultSet upstream;
  private final OResultMapper mapper;

  public OResultSetMapper(OResultSet upstream, OResultMapper mapper) {
    if (upstream == null || mapper == null) {
      throw new NullPointerException();
    }
    this.upstream = upstream;
    this.mapper = mapper;
  }

  @Override
  public boolean hasNext() {
    return upstream.hasNext();
  }

  @Override
  public OResult next() {
    return this.mapper.map(upstream.next());
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
