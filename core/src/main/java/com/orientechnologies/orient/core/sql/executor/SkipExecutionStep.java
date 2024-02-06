package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OSkip;

/** Created by luigidellaquila on 08/07/16. */
public class SkipExecutionStep extends AbstractExecutionStep {
  private final OSkip skip;

  public SkipExecutionStep(OSkip skip, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.skip = skip;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    int skipValue = skip.getValue(ctx);
    OExecutionStream rs = prev.get().start(ctx);
    int skipped = 0;
    while (rs.hasNext(ctx) && skipped < skipValue) {
      rs.next(ctx);
      skipped++;
    }

    return rs;
  }

  @Override
  public void sendTimeout() {}

  @Override
  public void close() {
    prev.ifPresent(x -> x.close());
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    return OExecutionStepInternal.getIndent(depth, indent) + "+ SKIP (" + skip.toString() + ")";
  }
}
