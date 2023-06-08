package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.sql.executor.resultset.OResultSetMapper;
import java.util.Optional;

/**
 * Deletes records coming from upstream steps
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class DeleteStep extends AbstractExecutionStep {
  private long cost = 0;

  public DeleteStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx) throws OTimeoutException {
    OResultSet upstream = getPrev().get().syncPull(ctx);
    return new OResultSetMapper(upstream, (result) -> this.mapResult(result, ctx));
  }

  private OResult mapResult(OResult result, OCommandContext ctx) {
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      Optional<ORID> id = result.getIdentity();
      if (id.isPresent()) {
        ctx.getDatabase().delete(id.get());
      }
      return result;
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
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
  public long getCost() {
    return cost;
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
