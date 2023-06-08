package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OResultSetMapper;
import com.orientechnologies.orient.core.sql.parser.OCluster;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;

/** Created by luigidellaquila on 14/02/17. */
public class MoveVertexStep extends AbstractExecutionStep {
  private String targetCluster;
  private String targetClass;

  public MoveVertexStep(
      OIdentifier targetClass,
      OCluster targetCluster,
      OCommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.targetClass = targetClass == null ? null : targetClass.getStringValue();
    if (targetCluster != null) {
      this.targetCluster = targetCluster.getClusterName();
      if (this.targetCluster == null) {
        this.targetCluster = ctx.getDatabase().getClusterNameById(targetCluster.getClusterNumber());
      }
    }
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx) throws OTimeoutException {
    OResultSet upstream = getPrev().get().syncPull(ctx);
    return new OResultSetMapper(upstream, this::mapResult);
  }

  private OResult mapResult(OResult result) {
    result.getVertex().ifPresent(x -> x.moveTo(targetClass, targetCluster));
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ MOVE VERTEX TO ");
    if (targetClass != null) {
      result.append("CLASS ");
      result.append(targetClass);
    }
    if (targetCluster != null) {
      result.append("CLUSTER ");
      result.append(targetCluster);
    }
    return result.toString();
  }
}
