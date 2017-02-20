package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;

import java.util.Map;
import java.util.Optional;

/**
 * Deletes records coming from upstream steps
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class DeleteStep extends AbstractExecutionStep {
  public DeleteStep(OCommandContext ctx) {
    super(ctx);
  }

  @Override public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    OResultSet upstream = getPrev().get().syncPull(ctx, nRecords);
    return new OResultSet() {
      @Override public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override public OResult next() {
        OResult result = upstream.next();
        if (result.isElement()) {
          result.getElement().get().delete();
        }
        return result;
      }

      @Override public void close() {
        upstream.close();
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
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ DELETE");
    return result.toString();
  }
}
