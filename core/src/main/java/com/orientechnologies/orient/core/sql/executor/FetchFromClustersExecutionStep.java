package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 21/07/16.
 */
public class FetchFromClustersExecutionStep extends AbstractExecutionStep {

  FetchFromClusterExecutionStep[] subSteps;
  OResultSet                      currentResultSet;
  private boolean orderByRidAsc  = false;
  private boolean orderByRidDesc = false;

  int currentStep = 0;

  /**
   * iterates over a class and its subclasses
   *
   * @param clusterIds the clusters
   * @param ctx       the query context
   * @param ridOrder  true to sort by RID asc, false to sort by RID desc, null for no sort.
   */
  public FetchFromClustersExecutionStep(int[] clusterIds, OCommandContext ctx, Boolean ridOrder) {
    super(ctx);

    if (Boolean.TRUE.equals(ridOrder)) {
      orderByRidAsc = true;
    } else if (Boolean.FALSE.equals(ridOrder)) {
      orderByRidDesc = true;
    }

    subSteps = new FetchFromClusterExecutionStep[clusterIds.length];
    sortClusers(clusterIds);
    for (int i = 0; i < clusterIds.length; i++) {
      FetchFromClusterExecutionStep step = new FetchFromClusterExecutionStep(clusterIds[i], ctx);
      if (orderByRidAsc) {
        step.setOrder(FetchFromClusterExecutionStep.ORDER_ASC);
      } else if (orderByRidDesc) {
        step.setOrder(FetchFromClusterExecutionStep.ORDER_DESC);
      }
      subSteps[i] = step;
    }
  }

  private void sortClusers(int[] clusterIds) {
    if (orderByRidAsc) {
      Arrays.sort(clusterIds);
    } else if (orderByRidDesc) {
      Arrays.sort(clusterIds);
      //revert order
      for (int i = 0; i < clusterIds.length / 2; i++) {
        int old = clusterIds[i];
        clusterIds[i] = clusterIds[clusterIds.length - 1 - i];
        clusterIds[clusterIds.length - 1 - i] = old;
      }
    }
  }

  @Override public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    return new OResultSet() {

      int totDispatched = 0;

      @Override public boolean hasNext() {
        while (true) {
          if (totDispatched >= nRecords) {
            return false;
          }
          if (currentResultSet == null || !currentResultSet.hasNext()) {
            if (currentStep >= subSteps.length) {
              return false;
            }
            currentResultSet = subSteps[currentStep].syncPull(ctx, nRecords);
            if (!currentResultSet.hasNext()) {
              currentResultSet = subSteps[currentStep++].syncPull(ctx, nRecords);
            }
          }
          if (!currentResultSet.hasNext()) {
            continue;
          }
          return true;
        }
      }

      @Override public OResult next() {
        while (true) {
          if (totDispatched >= nRecords) {
            throw new IllegalStateException();
          }
          if (currentResultSet == null || !currentResultSet.hasNext()) {
            if (currentStep >= subSteps.length) {
              throw new IllegalStateException();
            }
            currentResultSet = subSteps[currentStep].syncPull(ctx, nRecords);
            if (!currentResultSet.hasNext()) {
              currentResultSet = subSteps[currentStep++].syncPull(ctx, nRecords);
            }
          }
          if (!currentResultSet.hasNext()) {
            continue;
          }
          totDispatched++;
          return currentResultSet.next();
        }
      }

      @Override public void close() {
        for (FetchFromClusterExecutionStep step : subSteps) {
          step.close();
        }
      }

      @Override public Optional<OExecutionPlan> getExecutionPlan() {
        return Optional.empty();
      }

      @Override public Map<String, Long> getQueryStats() {
        return new HashMap<>();
      }
    };

  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override public void sendTimeout() {
    for (OExecutionStepInternal step : subSteps) {
      step.sendTimeout();
    }
    prev.ifPresent(p -> p.sendTimeout());
  }

  @Override public void close() {
    for (OExecutionStepInternal step : subSteps) {
      step.close();
    }
    prev.ifPresent(p -> p.close());
  }

  @Override public void sendResult(Object o, OExecutionCallback.Status status) {

  }

  @Override public String prettyPrint(int depth, int indent) {
    StringBuilder builder = new StringBuilder();
    String ind = OExecutionStepInternal.getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ FETCH FROM CLUSTERS \n");
    for (int i = 0; i < subSteps.length; i++) {
      OExecutionStepInternal step = subSteps[i];
      builder.append(step.prettyPrint(depth + 1, indent));
      if (i < subSteps.length - 1) {
        builder.append("\n");
      }
    }
    return builder.toString();
  }

}
