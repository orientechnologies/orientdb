package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Created by luigidellaquila on 16/03/17. */
public class GetValueFromIndexEntryStep extends AbstractExecutionStep {

  private final int[] filterClusterIds;

  // runtime

  private long cost = 0;

  private OResultSet prevResult = null;

  /**
   * @param ctx the execution context
   * @param filterClusterIds only extract values from these clusters. Pass null if no filtering is
   *     needed
   * @param profilingEnabled enable profiling
   */
  public GetValueFromIndexEntryStep(
      OCommandContext ctx, int[] filterClusterIds, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.filterClusterIds = filterClusterIds;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {

    if (!prev.isPresent()) {
      throw new IllegalStateException("filter step requires a previous step");
    }
    OExecutionStepInternal prevStep = prev.get();

    return new OResultSet() {

      public boolean finished = false;

      private OResult nextItem = null;
      private int fetched = 0;

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
        ctx.setVariable("$current", result);
        return result;
      }

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
          OResult val = prevResult.next();
          long begin = profilingEnabled ? System.nanoTime() : 0;

          try {
            Object finalVal = val.getProperty("rid");
            if (filterClusterIds != null) {
              if (!(finalVal instanceof OIdentifiable)) {
                continue;
              }
              ORID rid = ((OIdentifiable) finalVal).getIdentity();
              boolean found = false;
              for (int filterClusterId : filterClusterIds) {
                if (rid.getClusterId() < 0 || filterClusterId == rid.getClusterId()) {
                  found = true;
                  break;
                }
              }
              if (!found) {
                continue;
              }
            }
            if (finalVal instanceof OIdentifiable) {
              OResultInternal res = new OResultInternal((OIdentifiable) finalVal);
              nextItem = res;
            } else if (finalVal instanceof OResult) {
              nextItem = (OResult) finalVal;
            }
            break;
          } finally {
            if (profilingEnabled) {
              cost += (System.nanoTime() - begin);
            }
          }
        }
      }

      @Override
      public void close() {}

      @Override
      public Optional<OExecutionPlan> getExecutionPlan() {
        return Optional.empty();
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ EXTRACT VALUE FROM INDEX ENTRY";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    if (filterClusterIds != null) {
      result += "\n";
      result += spaces;
      result += "  filtering clusters [";
      result +=
          Arrays.stream(filterClusterIds).boxed().map(x -> "" + x).collect(Collectors.joining(","));
      result += "]";
    }
    return result;
  }

  @Override
  public long getCost() {
    return cost;
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new GetValueFromIndexEntryStep(ctx, this.filterClusterIds, this.profilingEnabled);
  }
}
