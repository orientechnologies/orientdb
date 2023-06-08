package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.resultset.OFilterResultSet;
import com.orientechnologies.orient.core.sql.executor.resultset.OLimitedResultSet;
import java.util.Set;
import java.util.stream.Collectors;

/** Created by luigidellaquila on 01/03/17. */
public class FilterByClustersStep extends AbstractExecutionStep {
  private Set<String> clusters;
  private Set<Integer> clusterIds;

  private OResultSet prevResult = null;

  public FilterByClustersStep(
      Set<String> filterClusters, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.clusters = filterClusters;
    ODatabaseSession db = ctx.getDatabase();
    init(db);
  }

  private void init(ODatabaseSession db) {
    if (this.clusterIds == null) {
      this.clusterIds =
          clusters.stream()
              .map(x -> db.getClusterIdByName(x))
              .filter(x -> x != null)
              .collect(Collectors.toSet());
    }
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    init(ctx.getDatabase());
    if (!prev.isPresent()) {
      throw new IllegalStateException("filter step requires a previous step");
    }
    OExecutionStepInternal prevStep = prev.get();

    return new OLimitedResultSet(
        new OFilterResultSet(
            () -> {
              if (prevResult == null) {
                prevResult = prevStep.syncPull(ctx, nRecords);
              } else if (!prevResult.hasNext()) {
                prevResult = prevStep.syncPull(ctx, nRecords);
              }
              return prevResult;
            },
            (result) -> {
              if (result.isElement()) {
                int clusterId = result.getIdentity().get().getClusterId();
                if (clusterId < 0) {
                  // this record comes from a TX, it still doesn't have a cluster assigned
                  return result;
                }
                if (clusterIds.contains(clusterId)) {
                  return result;
                }
              }
              return null;
            }),
        nRecords);
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    return OExecutionStepInternal.getIndent(depth, indent)
        + "+ FILTER ITEMS BY CLUSTERS \n"
        + OExecutionStepInternal.getIndent(depth, indent)
        + "  "
        + clusters.stream().collect(Collectors.joining(", "));
  }

  @Override
  public OResult serialize() {
    OResultInternal result = OExecutionStepInternal.basicSerialize(this);
    if (clusters != null) {
      result.setProperty("clusters", clusters);
    }

    return result;
  }

  @Override
  public void deserialize(OResult fromResult) {
    try {
      OExecutionStepInternal.basicDeserialize(fromResult, this);
      clusters = fromResult.getProperty("clusters");
    } catch (Exception e) {
      throw OException.wrapException(new OCommandExecutionException(""), e);
    }
  }
}
