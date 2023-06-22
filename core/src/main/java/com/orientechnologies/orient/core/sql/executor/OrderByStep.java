package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OOrderBy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Created by luigidellaquila on 11/07/16. */
public class OrderByStep extends AbstractExecutionStep {
  private final OOrderBy orderBy;
  private final long timeoutMillis;
  private Integer maxResults;

  public OrderByStep(
      OOrderBy orderBy, OCommandContext ctx, long timeoutMillis, boolean profilingEnabled) {
    this(orderBy, null, ctx, timeoutMillis, profilingEnabled);
  }

  public OrderByStep(
      OOrderBy orderBy,
      Integer maxResults,
      OCommandContext ctx,
      long timeoutMillis,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.orderBy = orderBy;
    this.maxResults = maxResults;
    if (this.maxResults != null && this.maxResults < 0) {
      this.maxResults = null;
    }
    this.timeoutMillis = timeoutMillis;
  }

  @Override
  public OExecutionStream syncPull(OCommandContext ctx) throws OTimeoutException {
    List<OResult> results;
    if (prev.isPresent()) {
      results = measure(ctx, (context) -> init(prev.get(), context));
    } else {
      results = Collections.emptyList();
    }
    return OExecutionStream.resultIterator(results.iterator());
  }

  private List<OResult> init(OExecutionStepInternal p, OCommandContext ctx) {
    long timeoutBegin = System.currentTimeMillis();
    List<OResult> cachedResult = new ArrayList<>();
    final long maxElementsAllowed =
        OGlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getValueAsLong();
    boolean sorted = true;
    OExecutionStream lastBatch = p.syncPull(ctx);
    while (lastBatch.hasNext(ctx)) {
      if (timeoutMillis > 0 && timeoutBegin + timeoutMillis < System.currentTimeMillis()) {
        sendTimeout();
      }

      OResult item = lastBatch.next(ctx);
      cachedResult.add(item);
      if (maxElementsAllowed >= 0 && maxElementsAllowed < cachedResult.size()) {
        throw new OCommandExecutionException(
            "Limit of allowed elements for in-heap ORDER BY in a single query exceeded ("
                + maxElementsAllowed
                + ") . You can set "
                + OGlobalConfiguration.QUERY_MAX_HEAP_ELEMENTS_ALLOWED_PER_OP.getKey()
                + " to increase this limit");
      }
      sorted = false;
      // compact, only at twice as the buffer, to avoid to do it at each add
      if (this.maxResults != null) {
        long compactThreshold = 2L * maxResults;
        if (compactThreshold < cachedResult.size()) {
          cachedResult.sort((a, b) -> orderBy.compare(a, b, ctx));
          cachedResult = new ArrayList<>(cachedResult.subList(0, maxResults));
          sorted = true;
        }
      }
    }
    // compact at each batch, if needed
    if (!sorted && this.maxResults != null && maxResults < cachedResult.size()) {
      cachedResult.sort((a, b) -> orderBy.compare(a, b, ctx));
      cachedResult = new ArrayList<>(cachedResult.subList(0, maxResults));
      sorted = true;
    }
    if (!sorted) {
      cachedResult.sort((a, b) -> orderBy.compare(a, b, ctx));
    }
    return cachedResult;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String result = OExecutionStepInternal.getIndent(depth, indent) + "+ " + orderBy;
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    result += (maxResults != null ? "\n  (buffer size: " + maxResults + ")" : "");
    return result;
  }
}
