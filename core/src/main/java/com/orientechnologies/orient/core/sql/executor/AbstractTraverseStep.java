package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.sql.parser.OInteger;
import com.orientechnologies.orient.core.sql.parser.OTraverseProjectionItem;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Created by luigidellaquila on 26/10/16. */
public abstract class AbstractTraverseStep extends AbstractExecutionStep {
  protected final OWhereClause whileClause;
  protected final List<OTraverseProjectionItem> projections;
  protected final OInteger maxDepth;

  private List<OResult> entryPoints = null;
  private List<OResult> results = new ArrayList<>();
  private long cost = 0;

  private Set<ORID> traversed = new ORidSet();

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
  public OResultSet syncPull(OCommandContext ctx) throws OTimeoutException {
    OResultSet resultSet = getPrev().get().syncPull(ctx);
    return new OResultSet() {

      @Override
      public boolean hasNext() {
        if (results.isEmpty()) {
          fetchNextBlock(ctx, resultSet);
        }
        if (results.isEmpty()) {
          return false;
        }
        return true;
      }

      @Override
      public OResult next() {
        if (results.isEmpty()) {
          fetchNextBlock(ctx, resultSet);
          if (results.isEmpty()) {
            throw new IllegalStateException();
          }
        }
        OResult result = results.remove(0);
        if (result.isElement()) {
          traversed.add(result.getElement().get().getIdentity());
        }
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
    };
  }

  private void fetchNextBlock(OCommandContext ctx, OResultSet resultSet) {
    if (this.entryPoints == null) {
      this.entryPoints = new ArrayList<OResult>();
    }
    if (!this.results.isEmpty()) {
      return;
    }
    while (this.results.isEmpty()) {
      if (this.entryPoints.isEmpty()) {
        fetchNextEntryPoints(resultSet, ctx, this.entryPoints, this.traversed);
      }
      if (this.entryPoints.isEmpty()) {
        return;
      }
      long begin = profilingEnabled ? System.nanoTime() : 0;
      fetchNextResults(ctx, this.results, this.entryPoints, this.traversed);
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
      if (!this.results.isEmpty()) {
        return;
      }
    }
  }

  protected abstract void fetchNextEntryPoints(
      OResultSet toFetch, OCommandContext ctx, List<OResult> entryPoints, Set<ORID> traversed);

  protected abstract void fetchNextResults(
      OCommandContext ctx, List<OResult> results, List<OResult> entryPoints, Set<ORID> traversed);

  protected boolean isFinished() {
    return entryPoints != null && entryPoints.isEmpty() && results.isEmpty();
  }

  @Override
  public long getCost() {
    return cost;
  }

  @Override
  public String toString() {
    return prettyPrint(0, 2);
  }
}
