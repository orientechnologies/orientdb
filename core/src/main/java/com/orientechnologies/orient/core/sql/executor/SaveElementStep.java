package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OResultSetMapper;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class SaveElementStep extends AbstractExecutionStep {

  private final OIdentifier cluster;

  public SaveElementStep(OCommandContext ctx, OIdentifier cluster, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.cluster = cluster;
  }

  public SaveElementStep(OCommandContext ctx, boolean profilingEnabled) {
    this(ctx, null, profilingEnabled);
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    OResultSet upstream = getPrev().get().syncPull(ctx);
    return new OResultSetMapper(upstream, (result) -> mapResult(ctx, result));
  }

  private OResult mapResult(OCommandContext ctx, OResult result) {
    if (result.isElement()) {
      if (cluster == null) {
        ctx.getDatabase().save(result.getElement().orElse(null));
      } else {
        ctx.getDatabase().save(result.getElement().orElse(null), cluster.getStringValue());
      }
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ SAVE RECORD");
    if (cluster != null) {
      result.append("\n");
      result.append(spaces);
      result.append("  on cluster " + cluster);
    }
    return result.toString();
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new SaveElementStep(ctx, cluster == null ? null : cluster.copy(), profilingEnabled);
  }
}
