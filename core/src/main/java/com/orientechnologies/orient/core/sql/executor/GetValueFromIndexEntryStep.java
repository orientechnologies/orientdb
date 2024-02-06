package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.Arrays;
import java.util.stream.Collectors;

/** Created by luigidellaquila on 16/03/17. */
public class GetValueFromIndexEntryStep extends AbstractExecutionStep {

  private final int[] filterClusterIds;

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
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {

    if (!prev.isPresent()) {
      throw new IllegalStateException("filter step requires a previous step");
    }
    OExecutionStream resultSet = prev.get().start(ctx);
    return resultSet.filter(this::filterMap);
  }

  private OResult filterMap(OResult result, OCommandContext ctx) {
    Object finalVal = result.getProperty("rid");
    if (filterClusterIds != null) {
      if (!(finalVal instanceof OIdentifiable)) {
        return null;
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
        return null;
      }
    }
    if (finalVal instanceof OIdentifiable) {
      return new OResultInternal((OIdentifiable) finalVal);

    } else if (finalVal instanceof OResult) {
      return (OResult) finalVal;
    }
    return null;
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
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new GetValueFromIndexEntryStep(ctx, this.filterClusterIds, this.profilingEnabled);
  }
}
