package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 22/07/16.
 */
public class FetchFromRidsStep extends AbstractExecutionStep {
  private final Collection<ORecordId> rids;

  private Iterator<ORecordId> iterator;

  private OResult nextResult = null;

  public FetchFromRidsStep(Collection<ORecordId> rids, OCommandContext ctx) {
    super(ctx);
    this.rids = rids;
    reset();
  }

  public void reset() {
    iterator = rids.iterator();
    nextResult = null;
  }

  @Override public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    return new OResultSet() {
      int internalNext = 0;

      private void fetchNext() {
        if (nextResult != null) {
          return;
        }
        while (iterator.hasNext()) {
          ORecordId nextRid = iterator.next();
          if (nextRid == null) {
            continue;
          }
          OIdentifiable nextDoc = (OIdentifiable) ctx.getDatabase().load(nextRid);
          if (nextDoc == null) {
            continue;
          }
          nextResult = new OResultInternal();
          ((OResultInternal) nextResult).setElement(nextDoc);
          return;
        }
        return;
      }

      @Override public boolean hasNext() {
        if (internalNext >= nRecords) {
          return false;
        }
        if (nextResult == null) {
          fetchNext();
        }
        return nextResult != null;
      }

      @Override public OResult next() {
        if (!hasNext()) {
          throw new IllegalStateException();
        }

        internalNext++;
        OResult result = nextResult;
        nextResult = null;
        return result;
      }

      @Override public void close() {

      }

      @Override public Optional<OExecutionPlan> getExecutionPlan() {
        return null;
      }

      @Override public Map<String, Long> getQueryStats() {
        return null;
      }
    };
  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override public void sendResult(Object o, Status status) {

  }

  @Override public String prettyPrint(int depth, int indent) {
    return OExecutionStepInternal.getIndent(depth, indent) + "+ FETCH FROM RIDs\n" +
        OExecutionStepInternal.getIndent(depth, indent) + "  " + rids;
  }
}
