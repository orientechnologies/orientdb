package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.MatchEdgeTraverser;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Map;
import java.util.Optional;

public final class OResultSetEdgeTraverser implements OResultSet {
  private final OCommandContext ctx;
  private final MatchEdgeTraverser trav;
  private OResult nextResult;

  public OResultSetEdgeTraverser(OCommandContext ctx, MatchEdgeTraverser trav) {
    this.ctx = ctx;
    this.trav = trav;
  }

  private void fetchNext() {
    if (nextResult == null) {
      while (trav.hasNext(ctx)) {
        nextResult = trav.next(ctx);
        if (nextResult != null) {
          break;
        }
      }
    }
  }

  @Override
  public OResult next() {
    fetchNext();
    OResult result = nextResult;
    ctx.setVariable("$matched", result);
    nextResult = null;
    return result;
  }

  @Override
  public boolean hasNext() {
    fetchNext();
    return nextResult != null;
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return null;
  }

  @Override
  public Optional<OExecutionPlan> getExecutionPlan() {
    return Optional.empty();
  }

  @Override
  public void close() {}
}
