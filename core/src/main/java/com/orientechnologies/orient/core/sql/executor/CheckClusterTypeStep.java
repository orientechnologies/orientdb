package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.parser.OCluster;

/**
 * This step is used just as a gate check to verify that a cluster belongs to a class.
 *
 * <p>It accepts two values: a target cluster (name or OCluster) and a class. If the cluster belongs
 * to the class, then the syncPool() returns an empty result set, otherwise it throws an
 * OCommandExecutionException
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila - at - orientdb.com)
 */
public class CheckClusterTypeStep extends AbstractExecutionStep {

  private OCluster cluster;
  private String clusterName;
  private String targetClass;
  private long cost = 0;
  private boolean found = false;

  public CheckClusterTypeStep(
      String targetClusterName, String clazz, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.clusterName = targetClusterName;
    this.targetClass = clazz;
  }

  public CheckClusterTypeStep(
      OCluster targetCluster, String clazz, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.cluster = targetCluster;
    this.targetClass = clazz;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      if (found) {
        return new OInternalResultSet();
      }
      ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) ctx.getDatabase();

      int clusterId;
      if (clusterName != null) {
        clusterId = db.getClusterIdByName(clusterName);
      } else if (cluster.getClusterName() != null) {
        clusterId = db.getClusterIdByName(cluster.getClusterName());
      } else {
        clusterId = cluster.getClusterNumber();
        if (db.getClusterNameById(clusterId) == null) {
          throw new OCommandExecutionException("Cluster not found: " + clusterId);
        }
      }
      if (clusterId < 0) {
        throw new OCommandExecutionException("Cluster not found: " + clusterName);
      }

      OClass clazz = db.getMetadata().getImmutableSchemaSnapshot().getClass(targetClass);
      if (clazz == null) {
        throw new OCommandExecutionException("Class not found: " + targetClass);
      }

      for (int clust : clazz.getPolymorphicClusterIds()) {
        if (clust == clusterId) {
          found = true;
          break;
        }
      }
      if (!found) {
        throw new OCommandExecutionException(
            "Cluster " + clusterId + " does not belong to class " + targetClass);
      }
      return new OInternalResultSet();
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ CHECK TARGET CLUSTER FOR CLASS");
    if (profilingEnabled) {
      result.append(" (" + getCostFormatted() + ")");
    }
    result.append("\n");
    result.append(spaces);
    result.append("  " + this.targetClass);
    return result.toString();
  }

  @Override
  public long getCost() {
    return cost;
  }
}
