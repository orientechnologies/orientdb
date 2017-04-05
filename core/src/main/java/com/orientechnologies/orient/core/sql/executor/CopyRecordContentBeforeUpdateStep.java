package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 08/08/16.
 */
public class CopyRecordContentBeforeUpdateStep extends AbstractExecutionStep {
  private long cost = 0;

  public CopyRecordContentBeforeUpdateStep(OCommandContext ctx) {
    super(ctx);
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    OResultSet lastFetched = getPrev().get().syncPull(ctx, nRecords);
    return new OResultSet() {
      @Override
      public boolean hasNext() {
        return lastFetched.hasNext();
      }

      @Override
      public OResult next() {
        OResult result = lastFetched.next();
        long begin = System.nanoTime();
        try {

          if (result instanceof OUpdatableResult) {
            OResultInternal prevValue = new OResultInternal();
            ORecord rec = result.getElement().get().getRecord();
            prevValue.setProperty("@rid", rec.getIdentity());
            prevValue.setProperty("@version", rec.getVersion());
            if (rec instanceof ODocument) {
              prevValue.setProperty("@class", ((ODocument) rec).getClass().getName());
            }
            for (String propName : result.getPropertyNames()) {
              prevValue.setProperty(propName, result.getProperty(propName));
            }
            ((OUpdatableResult) result).previousValue = prevValue;
          } else {
            throw new OCommandExecutionException("Cannot fetch previous value: " + result);
          }
          return result;
        } finally {
          cost += (System.nanoTime() - begin);
        }
      }

      @Override
      public void close() {
        lastFetched.close();
      }

      @Override
      public Optional<OExecutionPlan> getExecutionPlan() {
        return null;
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };
  }

  @Override
  public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override
  public void sendResult(Object o, Status status) {

  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ COPY RECORD CONTENT BEFORE UPDATE");
    return result.toString();
  }

  @Override
  public long getCost() {
    return cost;
  }

}
