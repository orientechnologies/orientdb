package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.Set;
import java.util.stream.Collectors;

/** Created by luigidellaquila on 01/03/17. */
public class FilterByClustersStep extends AbstractExecutionStep {
  private Set<String> clusters;

  public FilterByClustersStep(
      Set<String> filterClusters, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.clusters = filterClusters;
  }

  private Set<Integer> init(ODatabaseSession db) {
    return clusters.stream()
        .map(x -> db.getClusterIdByName(x))
        .filter(x -> x != null)
        .collect(Collectors.toSet());
  }

  @Override
  public OExecutionStream syncPull(OCommandContext ctx) throws OTimeoutException {
    Set<Integer> ids = init(ctx.getDatabase());
    if (!prev.isPresent()) {
      throw new IllegalStateException("filter step requires a previous step");
    }
    OExecutionStream resultSet = prev.get().syncPull(ctx);
    return resultSet.filter((value, context) -> this.filterMap(value, ids));
  }

  private OResult filterMap(OResult result, Set<Integer> clusterIds) {
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
