package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStreamProducer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Created by luigidellaquila on 08/07/16. */
public class FetchFromClassExecutionStep extends AbstractExecutionStep {

  protected String className;
  protected boolean orderByRidAsc = false;
  protected boolean orderByRidDesc = false;
  protected List<OExecutionStep> subSteps = new ArrayList<>();

  protected FetchFromClassExecutionStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  public FetchFromClassExecutionStep(
      String className,
      Set<String> clusters,
      OCommandContext ctx,
      Boolean ridOrder,
      boolean profilingEnabled) {
    this(className, clusters, null, ctx, ridOrder, profilingEnabled);
  }

  /**
   * iterates over a class and its subclasses
   *
   * @param className the class name
   * @param clusters if present (it can be null), filter by only these clusters
   * @param ctx the query context
   * @param ridOrder true to sort by RID asc, false to sort by RID desc, null for no sort.
   */
  public FetchFromClassExecutionStep(
      String className,
      Set<String> clusters,
      QueryPlanningInfo planningInfo,
      OCommandContext ctx,
      Boolean ridOrder,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);

    this.className = className;

    if (Boolean.TRUE.equals(ridOrder)) {
      orderByRidAsc = true;
    } else if (Boolean.FALSE.equals(ridOrder)) {
      orderByRidDesc = true;
    }
    OClass clazz = loadClassFromSchema(className, ctx);
    int[] classClusters = clazz.getPolymorphicClusterIds();
    List<Integer> filteredClassClusters = new ArrayList<>();
    for (int clusterId : classClusters) {
      String clusterName = ctx.getDatabase().getClusterNameById(clusterId);
      if (clusters == null || clusters.contains(clusterName)) {
        filteredClassClusters.add(clusterId);
      }
    }
    int[] clusterIds = new int[filteredClassClusters.size() + 1];
    for (int i = 0; i < filteredClassClusters.size(); i++) {
      clusterIds[i] = filteredClassClusters.get(i);
    }
    clusterIds[clusterIds.length - 1] = -1; // temporary cluster, data in tx

    sortClusers(clusterIds);
    for (int i = 0; i < clusterIds.length; i++) {
      int clusterId = clusterIds[i];
      if (clusterId > 0) {
        FetchFromClusterExecutionStep step =
            new FetchFromClusterExecutionStep(clusterId, planningInfo, ctx, profilingEnabled);
        if (orderByRidAsc) {
          step.setOrder(FetchFromClusterExecutionStep.ORDER_ASC);
        } else if (orderByRidDesc) {
          step.setOrder(FetchFromClusterExecutionStep.ORDER_DESC);
        }
        getSubSteps().add(step);
      } else {
        // current tx
        FetchTemporaryFromTxStep step =
            new FetchTemporaryFromTxStep(ctx, className, profilingEnabled);
        if (orderByRidAsc) {
          step.setOrder(FetchFromClusterExecutionStep.ORDER_ASC);
        } else if (orderByRidDesc) {
          step.setOrder(FetchFromClusterExecutionStep.ORDER_DESC);
        }
        getSubSteps().add(step);
      }
    }
  }

  protected OClass loadClassFromSchema(String className, OCommandContext ctx) {
    OClass clazz =
        ((ODatabaseDocumentInternal) ctx.getDatabase())
            .getMetadata()
            .getImmutableSchemaSnapshot()
            .getClass(className);
    if (clazz == null) {
      throw new OCommandExecutionException("Class " + className + " not found");
    }
    return clazz;
  }

  private void sortClusers(int[] clusterIds) {
    if (orderByRidAsc) {
      Arrays.sort(clusterIds);
    } else if (orderByRidDesc) {
      Arrays.sort(clusterIds);
      // revert order
      for (int i = 0; i < clusterIds.length / 2; i++) {
        int old = clusterIds[i];
        clusterIds[i] = clusterIds[clusterIds.length - 1 - i];
        clusterIds[clusterIds.length - 1 - i] = old;
      }
    }
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.start(ctx).close(ctx));

    List<OExecutionStep> stepsIter = getSubSteps();

    OExecutionStreamProducer res =
        new OExecutionStreamProducer() {
          private final Iterator<OExecutionStep> iter = stepsIter.iterator();

          @Override
          public OExecutionStream next(OCommandContext ctx) {
            OExecutionStep step = iter.next();
            return ((AbstractExecutionStep) step).start(ctx);
          }

          @Override
          public boolean hasNext(OCommandContext ctx) {
            return iter.hasNext();
          }

          @Override
          public void close(OCommandContext ctx) {}
        };

    return OExecutionStream.multiplStreams(res)
        .map(
            (result, context) -> {
              context.setVariable("$current", result);
              return result;
            });
  }

  @Override
  public void sendTimeout() {
    for (OExecutionStep step : getSubSteps()) {
      ((AbstractExecutionStep) step).sendTimeout();
    }
    prev.ifPresent(p -> p.sendTimeout());
  }

  @Override
  public void close() {
    for (OExecutionStep step : getSubSteps()) {
      ((AbstractExecutionStep) step).close();
    }
    prev.ifPresent(p -> p.close());
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    StringBuilder builder = new StringBuilder();
    String ind = OExecutionStepInternal.getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ FETCH FROM CLASS " + className);
    if (profilingEnabled) {
      builder.append(" (" + getCostFormatted() + ")");
    }
    builder.append("\n");
    for (int i = 0; i < getSubSteps().size(); i++) {
      OExecutionStepInternal step = (OExecutionStepInternal) getSubSteps().get(i);
      builder.append(step.prettyPrint(depth + 1, indent));
      if (i < getSubSteps().size() - 1) {
        builder.append("\n");
      }
    }
    return builder.toString();
  }

  @Override
  public OResult serialize() {
    OResultInternal result = OExecutionStepInternal.basicSerialize(this);
    result.setProperty("className", className);
    result.setProperty("orderByRidAsc", orderByRidAsc);
    result.setProperty("orderByRidDesc", orderByRidDesc);
    return result;
  }

  @Override
  public void deserialize(OResult fromResult) {
    try {
      OExecutionStepInternal.basicDeserialize(fromResult, this);
      this.className = fromResult.getProperty("className");
      this.orderByRidAsc = fromResult.getProperty("orderByRidAsc");
      this.orderByRidDesc = fromResult.getProperty("orderByRidDesc");
    } catch (Exception e) {
      throw OException.wrapException(new OCommandExecutionException(""), e);
    }
  }

  @Override
  public List<OExecutionStep> getSubSteps() {
    return subSteps;
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    FetchFromClassExecutionStep result = new FetchFromClassExecutionStep(ctx, profilingEnabled);
    result.className = this.className;
    result.orderByRidAsc = this.orderByRidAsc;
    result.orderByRidDesc = this.orderByRidDesc;
    result.subSteps =
        this.subSteps.stream()
            .map(x -> ((OExecutionStepInternal) x).copy(ctx))
            .collect(Collectors.toList());
    return result;
  }
}
