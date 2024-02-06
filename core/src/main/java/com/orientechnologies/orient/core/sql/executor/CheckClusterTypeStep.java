package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
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
  public OExecutionStream internalStart(OCommandContext context) throws OTimeoutException {
    getPrev().ifPresent(x -> x.start(context).close(ctx));
    ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) context.getDatabase();

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

    boolean found = false;
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
    return OExecutionStream.empty();
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
}
