package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORecordId;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Created by luigidellaquila on 22/07/16. */
public class FetchFromRidsStep extends AbstractExecutionStep {
  private Collection<ORecordId> rids;

  private Iterator<ORecordId> iterator;

  private OResult nextResult = null;

  public FetchFromRidsStep(
      Collection<ORecordId> rids, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.rids = rids;
    reset();
  }

  public void reset() {
    iterator = rids.iterator();
    nextResult = null;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    return new OResultSet() {
      private int internalNext = 0;

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
          nextResult = new OResultInternal(nextDoc);
          return;
        }
        return;
      }

      @Override
      public boolean hasNext() {
        if (internalNext >= nRecords) {
          return false;
        }
        if (nextResult == null) {
          fetchNext();
        }
        return nextResult != null;
      }

      @Override
      public OResult next() {
        if (!hasNext()) {
          throw new IllegalStateException();
        }

        internalNext++;
        OResult result = nextResult;
        nextResult = null;
        ctx.setVariable("$current", result.toElement());
        return result;
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
    return OExecutionStepInternal.getIndent(depth, indent)
        + "+ FETCH FROM RIDs\n"
        + OExecutionStepInternal.getIndent(depth, indent)
        + "  "
        + rids;
  }

  @Override
  public OResult serialize() {
    OResultInternal result = OExecutionStepInternal.basicSerialize(this);
    if (rids != null) {
      result.setProperty("rids", rids.stream().map(x -> x.toString()).collect(Collectors.toList()));
    }
    return result;
  }

  @Override
  public void deserialize(OResult fromResult) {
    try {
      OExecutionStepInternal.basicDeserialize(fromResult, this);
      if (fromResult.getProperty("rids") != null) {
        List<String> ser = fromResult.getProperty("rids");
        rids = ser.stream().map(x -> new ORecordId(x)).collect(Collectors.toList());
      }
      reset();
    } catch (Exception e) {
      throw OException.wrapException(new OCommandExecutionException(""), e);
    }
  }
}
