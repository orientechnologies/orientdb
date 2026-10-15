package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.config.OConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Created by luigidellaquila on 08/07/16. */
public class DistinctExecutionStep extends AbstractExecutionStep {

  private long maxElementsAllowed;

  public DistinctExecutionStep(OCommandContext ctx) {
    super();
    ODatabaseSession db = ctx == null ? null : ctx.getDatabase();

    maxElementsAllowed =
        db == null
            ? OConfiguration.global().queryMaxHeapElementsAllowedPerOp()
            : db.getConfiguration().queryMaxHeapElementsAllowedPerOp();
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    OExecutionStream resultSet = prev.get().start(ctx);
    // Hash-bucketed dedup, scanned with equals, to avoid HashSet tree-fallback through
    // OResultInternal.compareTo (which does not satisfy the Comparable contract).
    Map<Integer, List<OResult>> pastItems = new HashMap<>();
    long[] pastItemsSize = new long[] {0};
    ORidSet pastRids = new ORidSet();

    return resultSet.filter(
        (result, context) -> filterMap(context, result, pastRids, pastItems, pastItemsSize));
  }

  private OResult filterMap(
      OCommandContext ctx,
      OResult result,
      ORidSet pastRids,
      Map<Integer, List<OResult>> pastItems,
      long[] pastItemsSize) {
    if (alreadyVisited(result, pastRids, pastItems)) {
      return null;
    } else {
      markAsVisited(result, pastRids, pastItems, pastItemsSize);
      return result;
    }
  }

  private void markAsVisited(
      OResult nextValue,
      ORidSet pastRids,
      Map<Integer, List<OResult>> pastItems,
      long[] pastItemsSize) {
    if (nextValue.isElement()) {
      ORID identity = nextValue.getElement().get().getIdentity();
      int cluster = identity.getClusterId();
      long pos = identity.getClusterPosition();
      if (cluster >= 0 && pos >= 0) {
        pastRids.add(identity);
        return;
      }
    }
    pastItems.computeIfAbsent(nextValue.hashCode(), k -> new ArrayList<>()).add(nextValue);
    pastItemsSize[0]++;
    if (maxElementsAllowed > 0 && maxElementsAllowed < pastItemsSize[0]) {
      pastItems.clear();
      throw new OCommandExecutionException(
          "Limit of allowed elements for in-heap DISTINCT in a single query exceeded ("
              + maxElementsAllowed
              + ") . You can set "
              + OGlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey()
              + " to increase this limit");
    }
  }

  private boolean alreadyVisited(
      OResult nextValue, ORidSet pastRids, Map<Integer, List<OResult>> pastItems) {
    if (nextValue.isElement()) {
      ORID identity = nextValue.getElement().get().getIdentity();
      int cluster = identity.getClusterId();
      long pos = identity.getClusterPosition();
      if (cluster >= 0 && pos >= 0) {
        return pastRids.contains(identity);
      }
    }
    List<OResult> bucket = pastItems.get(nextValue.hashCode());
    if (bucket == null) {
      return false;
    }
    for (OResult past : bucket) {
      if (past.equals(nextValue)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    String result = OExecutionStepInternal.getIndent(ctx) + "+ DISTINCT";
    if (ctx.isProfilingEnabled()) {
      result += " (" + ctx.getCostFormatted(this) + ")";
    }
    return result;
  }
}
