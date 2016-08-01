package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OOrderBy;

import java.util.*;

/**
 * Created by luigidellaquila on 11/07/16.
 */
public class OrderByStep extends AbstractExecutionStep {
  private final OOrderBy orderBy;

  List<OResult> cachedResult = null;
  int                   nextElement  = 0;

  public OrderByStep(OOrderBy orderBy, OCommandContext ctx) {
    super(ctx);
    this.orderBy = orderBy;
  }

  @Override public OTodoResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    if (cachedResult == null) {
      cachedResult = new ArrayList<>();
      prev.ifPresent(p -> init(p, ctx));
    }

    return new OTodoResultSet() {
      int currentBatchReturned = 0;
      int offset = nextElement;

      @Override public boolean hasNext() {
        if (currentBatchReturned >= nRecords) {
          return false;
        }
        if (cachedResult.size() <= nextElement) {
          return false;
        }
        return true;
      }

      @Override public OResult next() {
        if (currentBatchReturned >= nRecords) {
          throw new IllegalStateException();
        }
        if (cachedResult.size() <= nextElement) {
          throw new IllegalStateException();
        }
        OResult result = cachedResult.get(offset + currentBatchReturned);
        nextElement++;
        currentBatchReturned++;
        return result;
      }

      @Override public void close() {
        prev.ifPresent(p -> p.close());
      }

      @Override public Optional<OExecutionPlan> getExecutionPlan() {
        return Optional.empty();
      }

      @Override public Map<String, Object> getQueryStats() {
        return new HashMap<>();
      }
    };
  }

  private void init(OExecutionStepInternal p, OCommandContext ctx) {
    do {
      OTodoResultSet lastBatch = p.syncPull(ctx, 100);
      if (!lastBatch.hasNext()) {
        break;
      }
      while (lastBatch.hasNext()) {
        if (this.timedOut) {
          break;
        }
        cachedResult.add(lastBatch.next());
      }
    } while (true);
    cachedResult.sort((a, b) -> orderBy.compare(a, b, ctx));
  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override public void sendResult(Object o, Status status) {

  }

  @Override public String prettyPrint(int depth, int indent) {
    return OExecutionStepInternal.getIndent(depth, indent) + "+ "+orderBy;
  }
}
