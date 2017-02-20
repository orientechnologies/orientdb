package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.OEdge;

import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 20/02/17.
 */
public class CastToEdgeStep extends AbstractExecutionStep {
  public CastToEdgeStep(OCommandContext ctx) {
    super(ctx);
  }

  @Override
  public void sendResult(Object o, Status status) {

  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    OResultSet upstream = getPrev().get().syncPull(ctx, nRecords);
    return new OResultSet() {

      @Override
      public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override
      public OResult next() {
        OResult result = upstream.next();
        if (result.getElement().orElse(null) instanceof OEdge) {
          return result;
        }
        if (result.isEdge()) {
          if (result instanceof OResultInternal) {
            ((OResultInternal) result).setElement(result.getElement().get().asEdge().get());
          } else {
            OResultInternal r = new OResultInternal();
            r.setElement(result.getElement().get().asEdge().get());
            result = r;
          }
        } else {
          throw new OCommandExecutionException("Current element is not a vertex: " + result);
        }
        return result;
      }

      @Override
      public void close() {
        upstream.close();
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
}
