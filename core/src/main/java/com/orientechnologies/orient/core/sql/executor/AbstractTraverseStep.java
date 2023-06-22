package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OInteger;
import com.orientechnologies.orient.core.sql.parser.OTraverseProjectionItem;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Created by luigidellaquila on 26/10/16. */
public abstract class AbstractTraverseStep extends AbstractExecutionStep {
  protected final OWhereClause whileClause;
  protected final List<OTraverseProjectionItem> projections;
  protected final OInteger maxDepth;

  public AbstractTraverseStep(
      List<OTraverseProjectionItem> projections,
      OWhereClause whileClause,
      OInteger maxDepth,
      OCommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.whileClause = whileClause;
    this.maxDepth = maxDepth;

    try (final Stream<OTraverseProjectionItem> stream = projections.stream()) {
      this.projections = stream.map(OTraverseProjectionItem::copy).collect(Collectors.toList());
    }
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    OExecutionStream resultSet = getPrev().get().start(ctx);
    return new OExecutionStream() {
      private List<OResult> entryPoints = new ArrayList<>();
      private List<OResult> results = new ArrayList<>();
      private Set<ORID> traversed = new ORidSet();

      @Override
      public boolean hasNext(OCommandContext ctx) {
        if (results.isEmpty()) {
          fetchNextBlock(ctx, this.entryPoints, this.results, this.traversed, resultSet);
        }
        if (results.isEmpty()) {
          return false;
        }
        return true;
      }

      @Override
      public OResult next(OCommandContext ctx) {
        if (!hasNext(ctx)) {
          throw new IllegalStateException();
        }
        OResult result = results.remove(0);
        if (result.isElement()) {
          this.traversed.add(result.getElement().get().getIdentity());
        }
        return result;
      }

      @Override
      public void close(OCommandContext ctx) {}
    };
  }

  private void fetchNextBlock(
      OCommandContext ctx,
      List<OResult> entryPoints,
      List<OResult> results,
      Set<ORID> traversed,
      OExecutionStream resultSet) {
    if (!results.isEmpty()) {
      return;
    }
    while (results.isEmpty()) {
      if (entryPoints.isEmpty()) {
        fetchNextEntryPoints(resultSet, ctx, entryPoints, traversed);
      }
      if (entryPoints.isEmpty()) {
        return;
      }
      fetchNextResults(ctx, results, entryPoints, traversed);
      if (!results.isEmpty()) {
        return;
      }
    }
  }

  protected abstract void fetchNextEntryPoints(
      OExecutionStream toFetch,
      OCommandContext ctx,
      List<OResult> entryPoints,
      Set<ORID> traversed);

  protected abstract void fetchNextResults(
      OCommandContext ctx, List<OResult> results, List<OResult> entryPoints, Set<ORID> traversed);

  @Override
  public String toString() {
    return prettyPrint(0, 2);
  }
}
