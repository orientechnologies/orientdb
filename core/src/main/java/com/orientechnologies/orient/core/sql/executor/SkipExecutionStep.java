package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OSkip;

/** Created by luigidellaquila on 08/07/16. */
public class SkipExecutionStep extends AbstractExecutionStep {
  private final OSkip skip;

  private int skipped = 0;

  private boolean finished;

  public SkipExecutionStep(OSkip skip, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.skip = skip;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx) throws OTimeoutException {
    if (finished == true) {
      return new OInternalResultSet(); // empty
    }
    int skipValue = skip.getValue(ctx);
    OResultSet rs = prev.get().syncPull(ctx);

    while (rs.hasNext() && skipped < skipValue) {
      rs.next();
      skipped++;
    }
    if (!rs.hasNext()) {
      finished = true;
      return new OInternalResultSet(); // empty
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
