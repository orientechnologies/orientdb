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
  public CopyRecordContentBeforeUpdateStep(OCommandContext ctx) {
    super(ctx);
  }

  @Override public OTodoResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    OTodoResultSet lastFetched = getPrev().get().syncPull(ctx, nRecords);
    return new OTodoResultSet() {
      @Override public boolean hasNext() {
        return lastFetched.hasNext();
      }

      @Override public OResult next() {
        OResult result = lastFetched.next();
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
      }

      @Override public void close() {
        lastFetched.close();
      }

      @Override public Optional<OExecutionPlan> getExecutionPlan() {
        return null;
      }

      @Override public Map<String, Object> getQueryStats() {
        return null;
      }
    };
  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override public void sendResult(Object o, Status status) {

  }
}
