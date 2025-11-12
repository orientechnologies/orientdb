package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Created by luigidellaquila on 21/07/16. */
public class FetchFromClustersExecutionStep extends AbstractExecutionStep {

  private List<OExecutionStepInternal> subSteps;
  private boolean orderByRidAsc = false;
  private boolean orderByRidDesc = false;

  /**
   * iterates over a class and its subclasses
   *
   * @param clusterIds the clusters
   * @param ridOrder true to sort by RID asc, false to sort by RID desc, null for no sort.
   */
  public FetchFromClustersExecutionStep(int[] clusterIds, Boolean ridOrder) {
    super();

    if (Boolean.TRUE.equals(ridOrder)) {
      orderByRidAsc = true;
    } else if (Boolean.FALSE.equals(ridOrder)) {
      orderByRidDesc = true;
    }

    subSteps = new ArrayList<>();
    sortClusers(clusterIds);
    for (int i = 0; i < clusterIds.length; i++) {
      FetchFromClusterExecutionStep step = new FetchFromClusterExecutionStep(clusterIds[i]);
      if (orderByRidAsc) {
        step.setOrder(FetchFromClusterExecutionStep.ORDER_ASC);
      } else if (orderByRidDesc) {
        step.setOrder(FetchFromClusterExecutionStep.ORDER_DESC);
      }
      subSteps.add(step);
    }
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

    List<OExecutionStepInternal> stepsIter = getSubSteps();

    return OExecutionStream.streamsFromIterator(stepsIter.iterator(), this::startStep);
  }

  private OExecutionStream startStep(OExecutionStepInternal step, OCommandContext context) {
    return ((AbstractExecutionStep) step).start(context);
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    StringBuilder builder = new StringBuilder();
    String ind = OExecutionStepInternal.getIndent(ctx);
    builder.append(ind);
    builder.append("+ FETCH FROM CLUSTERS");
    if (ctx.isProfilingEnabled()) {
      builder.append(" (" + ctx.getCostFormatted(this) + ")");
    }
    builder.append("\n");
    for (int i = 0; i < subSteps.size(); i++) {
      OExecutionStepInternal step = subSteps.get(i);
      builder.append(step.prettyPrint(ctx));
      if (i < subSteps.size() - 1) {
        builder.append("\n");
      }
    }
    return builder.toString();
  }

  @Override
  public List<OExecutionStepInternal> getSubSteps() {
    return subSteps;
  }

  @Override
  public OResult serialize() {
    OResultInternal result = OExecutionStepInternal.basicSerialize(this);
    result.setProperty("orderByRidAsc", orderByRidAsc);
    result.setProperty("orderByRidDesc", orderByRidDesc);
    return result;
  }

  @Override
  public void deserialize(OResult fromResult) {
    try {
      OExecutionStepInternal.basicDeserialize(fromResult, this);
      this.orderByRidAsc = fromResult.getProperty("orderByRidAsc");
      this.orderByRidDesc = fromResult.getProperty("orderByRidDesc");
    } catch (Exception e) {
      throw OException.wrapException(new OCommandExecutionException(""), e);
    }
  }
}
