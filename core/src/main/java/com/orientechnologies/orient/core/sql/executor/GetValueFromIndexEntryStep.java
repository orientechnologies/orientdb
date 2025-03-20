package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import java.util.Arrays;
import java.util.stream.Collectors;

/** Created by luigidellaquila on 16/03/17. */
public class GetValueFromIndexEntryStep extends AbstractExecutionStep {

  private final int[] filterClusterIds;

  /**
   * @param filterClusterIds only extract values from these clusters. Pass null if no filtering is
   *     needed
   */
  public GetValueFromIndexEntryStep(int[] filterClusterIds) {
    super();
    this.filterClusterIds = filterClusterIds;
    if (this.filterClusterIds != null) {
      Arrays.sort(this.filterClusterIds);
    }
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
      if (rid.getClusterId() >= 0
          && Arrays.binarySearch(filterClusterIds, rid.getClusterId()) < 0) {
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
  public String prettyPrint(OPrintContext ctx) {
    String spaces = OExecutionStepInternal.getIndent(ctx);
    String result = spaces + "+ EXTRACT VALUE FROM INDEX ENTRY";
    if (ctx.isProfilingEnabled()) {
      result += " (" + ctx.getCostFormatted(this) + ")";
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
  public OExecutionStepInternal copy(OCommandContext ctx) {
    return new GetValueFromIndexEntryStep(this.filterClusterIds);
  }
}
