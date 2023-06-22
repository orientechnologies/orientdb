package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.HashSet;
import java.util.Set;

/** Created by luigidellaquila on 08/07/16. */
public class DistinctExecutionStep extends AbstractExecutionStep {

  private long maxElementsAllowed;

  public DistinctExecutionStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    ODatabaseSession db = ctx == null ? null : ctx.getDatabase();

    maxElementsAllowed =
        db == null
            ? OGlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getValueAsLong()
            : db.getConfiguration()
                .getValueAsLong(OGlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP);
  }

  @Override
  public OExecutionStream syncPull(OCommandContext ctx) throws OTimeoutException {
    OExecutionStream resultSet = prev.get().syncPull(ctx);
    Set<OResult> pastItems = new HashSet<>();
    ORidSet pastRids = new ORidSet();

    return attachProfile(
        resultSet.filter((result, context) -> filterMap(context, result, pastRids, pastItems)));
  }

  private OResult filterMap(
      OCommandContext ctx, OResult result, Set<ORID> pastRids, Set<OResult> pastItems) {
    if (alreadyVisited(result, pastRids, pastItems)) {
      return null;
    } else {
      markAsVisited(result, pastRids, pastItems);
      return result;
    }
  }

  private void markAsVisited(OResult nextValue, Set<ORID> pastRids, Set<OResult> pastItems) {
    if (nextValue.isElement()) {
      ORID identity = nextValue.getElement().get().getIdentity();
      int cluster = identity.getClusterId();
      long pos = identity.getClusterPosition();
      if (cluster >= 0 && pos >= 0) {
        pastRids.add(identity);
        return;
      }
    }
    pastItems.add(nextValue);
    if (maxElementsAllowed > 0 && maxElementsAllowed < pastItems.size()) {
      pastItems.clear();
      throw new OCommandExecutionException(
          "Limit of allowed elements for in-heap DISTINCT in a single query exceeded ("
              + maxElementsAllowed
              + ") . You can set "
              + OGlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey()
              + " to increase this limit");
    }
  }

  private boolean alreadyVisited(OResult nextValue, Set<ORID> pastRids, Set<OResult> pastItems) {
    if (nextValue.isElement()) {
      ORID identity = nextValue.getElement().get().getIdentity();
      int cluster = identity.getClusterId();
      long pos = identity.getClusterPosition();
      if (cluster >= 0 && pos >= 0) {
        return pastRids.contains(identity);
      }
    }
    return pastItems.contains(nextValue);
  }

  @Override
  public void sendTimeout() {}

  @Override
  public void close() {
    prev.ifPresent(x -> x.close());
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String result = OExecutionStepInternal.getIndent(depth, indent) + "+ DISTINCT";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }
}
