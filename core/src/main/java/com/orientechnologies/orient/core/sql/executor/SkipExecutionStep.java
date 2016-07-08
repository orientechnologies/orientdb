package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OSkip;

/**
 * Created by luigidellaquila on 08/07/16.
 */
public class SkipExecutionStep extends AbstractExecutionStep {
  private final OSkip skip;

  int skipped = 0;

  OTodoResultSet lastFetch;
  private boolean finished;

  public SkipExecutionStep(OSkip skip, OCommandContext ctx) {
    super(ctx);
    this.skip = skip;
  }

  @Override public OTodoResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    if (finished == true) {
      return new OInternalResultSet();//empty
    }
    int skipValue = skip.getValue(ctx);
    while (skipped < skipValue) {
      //fetch and discard
      OTodoResultSet rs = prev.get().syncPull(ctx, Math.min(100, skipValue - skipped));//fetch blocks of 100, at most
      if (!rs.hasNext()) {
        finished = true;
        return new OInternalResultSet();//empty
      }
      while (rs.hasNext()) {
        rs.next();
        skipped++;
      }
    }

    return prev.get().syncPull(ctx, nRecords);

  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override public void sendTimeout() {

  }

  @Override public void close() {
    prev.ifPresent(x -> x.close());
  }

  @Override public void sendResult(Object o, Status status) {

  }

  @Override public String prettyPrint(int depth, int indent) {
    return OExecutionStep.getIndent(depth, indent) + "SKIP (" + skip.toString() + ")";
  }

}
