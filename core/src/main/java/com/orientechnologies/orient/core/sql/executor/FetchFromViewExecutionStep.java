package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OView;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Created by luigidellaquila on 08/07/16. */
public class FetchFromViewExecutionStep extends FetchFromClassExecutionStep {

  private List<Integer> usedClusters = new ArrayList<>();

  public FetchFromViewExecutionStep(
      String className,
      Set<String> clusters,
      QueryPlanningInfo planningInfo,
      OCommandContext ctx,
      Boolean ridOrder) {
    super(className, clusters, planningInfo, ctx, ridOrder);

    ODatabaseDocumentInternal database = (ODatabaseDocumentInternal) ctx.getDatabase();
    OView view = loadClassFromSchema(className, ctx);
    int[] classClusters = view.getPolymorphicClusterIds();
    for (int clusterId : classClusters) {
      String clusterName = ctx.getDatabase().getClusterNameById(clusterId);
      if (clusters == null || clusters.contains(clusterName)) {
        usedClusters.add(clusterId);
        database.queryStartUsingViewCluster(clusterId);
      }
    }
  }

  protected OView loadClassFromSchema(String className, OCommandContext ctx) {
    OView clazz =
        ((ODatabaseDocumentInternal) ctx.getDatabase())
            .getMetadata()
            .getImmutableSchemaSnapshot()
            .getView(className);
    if (clazz == null) {
      throw new OCommandExecutionException("View " + className + " not found");
    }
    return clazz;
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    StringBuilder builder = new StringBuilder();
    String ind = OExecutionStepInternal.getIndent(ctx);
    builder.append(ind);
    builder.append("+ FETCH FROM VIEW " + className);
    if (ctx.isProfilingEnabled()) {
      builder.append(" (" + ctx.getCostFormatted(this) + ")");
    }
    builder.append("\n");
    for (int i = 0; i < getSubSteps().size(); i++) {
      OExecutionStepInternal step = getSubSteps().get(i);
      ctx.incDepth();
      builder.append(step.prettyPrint(ctx));
      ctx.decDepth();
      if (i < getSubSteps().size() - 1) {
        builder.append("\n");
      }
    }
    return builder.toString();
  }

  @Override
  public boolean canBeCached() {
    return false;
  }
}
