package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.Optional;

/**
 * Deletes records coming from upstream steps
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class DeleteStep extends AbstractExecutionStep {

  public DeleteStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OExecutionStream syncPull(OCommandContext ctx) throws OTimeoutException {
    OExecutionStream upstream = getPrev().get().syncPull(ctx);
    return attachProfile(upstream.map(this::mapResult));
  }

  private OResult mapResult(OResult result, OCommandContext ctx) {
    Optional<ORID> id = result.getIdentity();
    if (id.isPresent()) {
      ctx.getDatabase().delete(id.get());
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ DELETE");
    if (profilingEnabled) {
      result.append(" (" + getCostFormatted() + ")");
    }
    return result.toString();
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new DeleteStep(ctx, this.profilingEnabled);
  }

  @Override
  public boolean canBeCached() {
    return true;
  }
}
