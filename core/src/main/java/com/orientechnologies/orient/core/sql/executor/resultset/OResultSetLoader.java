package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

public final class OResultSetLoader implements OResultSet {
  private final OCommandContext ctx;
  private OResult nextResult = null;
  private final Iterator<ORecordId> iterator;

  public OResultSetLoader(OCommandContext ctx, Iterator<ORecordId> iterator) {
    this.ctx = ctx;
    this.iterator = iterator;
  }

  private void fetchNext() {
    if (nextResult != null) {
      return;
    }
    while (iterator.hasNext()) {
      ORecordId nextRid = iterator.next();
      if (nextRid == null) {
        continue;
      }
      OIdentifiable nextDoc = (OIdentifiable) ctx.getDatabase().load(nextRid);
      if (nextDoc == null) {
        continue;
      }
      nextResult = new OResultInternal(nextDoc);
      return;
    }
    return;
  }

  @Override
  public boolean hasNext() {
    if (nextResult == null) {
      fetchNext();
    }
    return nextResult != null;
  }

  @Override
  public OResult next() {
    if (!hasNext()) {
      throw new IllegalStateException();
    }

    OResult result = nextResult;
    nextResult = null;
    ctx.setVariable("$current", result.toElement());
    return result;
  }

  @Override
  public void close() {}

  @Override
  public Optional<OExecutionPlan> getExecutionPlan() {
    return Optional.empty();
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return null;
  }
}
