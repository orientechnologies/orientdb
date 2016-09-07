package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.parser.OCluster;

/**
 * Created by luigidellaquila on 05/09/16.
 */
public class CheckClusterTypeStep extends AbstractExecutionStep {

  OCluster cluster;
  String   clusterName;

  String targetClass;

  boolean found = false;

  public CheckClusterTypeStep(String targetClusterName, String clazz, OCommandContext ctx) {
    super(ctx);
    this.clusterName = targetClusterName;
    this.targetClass = clazz;
  }

  public CheckClusterTypeStep(OCluster targetCluster, String clazz, OCommandContext ctx) {
    super(ctx);
    this.cluster = targetCluster;
    this.targetClass = clazz;

  }

  @Override public OTodoResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    if (found) {
      return new OInternalResultSet();
    }
    ODatabase db = ctx.getDatabase();

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

    OClass clazz = db.getMetadata().getSchema().getClass(targetClass);
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
      throw new OCommandExecutionException("Cluster " + clusterId + " does not belong to class " + targetClass);
    }
    return new OInternalResultSet();
  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override public void sendResult(Object o, Status status) {

  }

  @Override public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ CHECK TARGET CLUSTER FOR CLASS\n");
    result.append("  " + this.targetClass);
    return result.toString();
  }
}
