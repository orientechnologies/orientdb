package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OCluster;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import java.util.Map;
import java.util.Optional;

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
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    OResultSet upstream = getPrev().get().syncPull(ctx, nRecords);
    return new OResultSet() {
      @Override
      public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override
      public OResult next() {
        OResult current = upstream.next();
        current.getVertex().ifPresent(x -> x.moveTo(targetClass, targetCluster));
        return current;
      }

      @Override
      public void close() {
        upstream.close();
      }

      @Override
      public Optional<OExecutionPlan> getExecutionPlan() {
        return Optional.empty();
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };
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
