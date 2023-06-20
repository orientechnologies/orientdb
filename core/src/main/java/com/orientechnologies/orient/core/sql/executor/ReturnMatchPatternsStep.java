package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;

/** Created by luigidellaquila on 12/10/16. */
public class ReturnMatchPatternsStep extends AbstractExecutionStep {

  public ReturnMatchPatternsStep(OCommandContext context, boolean profilingEnabled) {
    super(context, profilingEnabled);
  }

  @Override
  public OExecutionStream syncPull(OCommandContext ctx) throws OTimeoutException {
    OExecutionStream upstream = getPrev().get().syncPull(ctx);
    return upstream.map(this::mapResult);
  }

  private OResult mapResult(OResult next, OCommandContext ctx) {
    next.getPropertyNames().stream()
        .filter(s -> s.startsWith(OMatchExecutionPlanner.DEFAULT_ALIAS_PREFIX))
        .forEach(((OResultInternal) next)::removeProperty);
    return next;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ RETURN $patterns";
  }
}
