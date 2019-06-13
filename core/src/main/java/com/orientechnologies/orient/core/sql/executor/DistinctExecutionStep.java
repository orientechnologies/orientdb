package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.id.ORID;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Created by luigidellaquila on 08/07/16.
 */
public class DistinctExecutionStep extends AbstractExecutionStep {

  Set<OResult> pastItems = new HashSet<>();
  ORidSet      pastRids  = new ORidSet();

  OResultSet lastResult = null;
  OResult nextValue;

  private long cost = 0;

  public DistinctExecutionStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {

    OResultSet result = new OResultSet() {
      int nextLocal = 0;

      @Override
      public boolean hasNext() {
        if (nextLocal >= nRecords) {
          return false;
        }
        if (nextValue != null) {
          return true;
        }
        fetchNext(nRecords);
        return nextValue != null;
      }

      @Override
      public OResult next() {
        if (nextLocal >= nRecords) {
          throw new IllegalStateException();
        }
        if (nextValue == null) {
          fetchNext(nRecords);
        }
        if (nextValue == null) {
          throw new IllegalStateException();
        }
        OResult result = nextValue;
        nextValue = null;
        nextLocal++;
        return result;
      }

      @Override
      public void close() {

      }

      @Override
      public Optional<OExecutionPlan> getExecutionPlan() {
        return Optional.empty();
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };

    return result;
  }

  private void fetchNext(int nRecords) {
    while (true) {
      if (nextValue != null) {
        return;
      }
      if (lastResult == null || !lastResult.hasNext()) {
        lastResult = getPrev().get().syncPull(ctx, nRecords);
      }
      if (lastResult == null || !lastResult.hasNext()) {
        return;
      }
      long begin = profilingEnabled ? System.nanoTime() : 0;
      try {
        nextValue = lastResult.next();
        if (alreadyVisited(nextValue)) {
          nextValue = null;
        } else {
          markAsVisited(nextValue);
        }
      } finally {
        if (profilingEnabled) {
          cost += (System.nanoTime() - begin);
        }
      }
    }
  }

  private void markAsVisited(OResult nextValue) {
    if (nextValue.isElement()) {
      ORID identity = nextValue.getElement().get().getIdentity();
      int cluster = identity.getClusterId();
      long pos = identity.getClusterPosition();
      if (cluster >= 0 && pos >= 0) {
        pastRids.add(identity);
        return;
      }
    }
    pastItems.add(nextValue);
  }

  private boolean alreadyVisited(OResult nextValue) {
    if (nextValue.isElement()) {
      ORID identity = nextValue.getElement().get().getIdentity();
      int cluster = identity.getClusterId();
      long pos = identity.getClusterPosition();
      if (cluster >= 0 && pos >= 0) {
        return pastRids.contains(identity);
      }
    }
    return pastItems.contains(nextValue);
  }

  @Override
  public void sendTimeout() {

  }

  @Override
  public void close() {
    prev.ifPresent(x -> x.close());
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String result = OExecutionStepInternal.getIndent(depth, indent) + "+ DISTINCT";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

  @Override
  public long getCost() {
    return cost;
  }
}
