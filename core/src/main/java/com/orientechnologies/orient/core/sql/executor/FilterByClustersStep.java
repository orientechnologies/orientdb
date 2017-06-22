package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabase;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by luigidellaquila on 01/03/17.
 */
public class FilterByClustersStep extends AbstractExecutionStep {
  private final Set<String>  clusters;
  private final Set<Integer> clusterIds;

  OResultSet prevResult = null;

  public FilterByClustersStep(Set<String> filterClusters, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.clusters = filterClusters;
    ODatabase db = ctx.getDatabase();
    this.clusterIds = clusters.stream().map(x -> db.getClusterIdByName(x)).filter(x -> x != null).collect(Collectors.toSet());
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    if (!prev.isPresent()) {
      throw new IllegalStateException("filter step requires a previous step");
    }
    OExecutionStepInternal prevStep = prev.get();

    return new OResultSet() {
      public boolean finished = false;

      OResult nextItem = null;
      int fetched = 0;

      private void fetchNextItem() {
        nextItem = null;
        if (finished) {
          return;
        }
        if (prevResult == null) {
          prevResult = prevStep.syncPull(ctx, nRecords);
          if (!prevResult.hasNext()) {
            finished = true;
            return;
          }
        }
        while (!finished) {
          while (!prevResult.hasNext()) {
            prevResult = prevStep.syncPull(ctx, nRecords);
            if (!prevResult.hasNext()) {
              finished = true;
              return;
            }
          }
          nextItem = prevResult.next();
          if (nextItem.isElement()) {
            if (clusterIds.contains(nextItem.getIdentity().get().getClusterId())) {
              break;
            }
          } nextItem = null;
        }
      }

      @Override
      public boolean hasNext() {

        if (fetched >= nRecords || finished) {
          return false;
        }
        if (nextItem == null) {
          fetchNextItem();
        }

        if (nextItem != null) {
          return true;
        }

        return false;
      }

      @Override
      public OResult next() {
        if (fetched >= nRecords || finished) {
          throw new IllegalStateException();
        }
        if (nextItem == null) {
          fetchNextItem();
        }
        if (nextItem == null) {
          throw new IllegalStateException();
        }
        OResult result = nextItem;
        nextItem = null;
        fetched++;
        return result;
      }

      @Override
      public void close() {
        FilterByClustersStep.this.close();
      }

      @Override
      public Optional<OExecutionPlan> getExecutionPlan() {
        return null;
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };

  }

  @Override
  public String prettyPrint(int depth, int indent) {
    return OExecutionStepInternal.getIndent(depth, indent) + "+ FILTER ITEMS BY CLUSTERS \n" + OExecutionStepInternal
        .getIndent(depth, indent) + "  " + clusters.stream().collect(Collectors.joining(", "));
  }

}