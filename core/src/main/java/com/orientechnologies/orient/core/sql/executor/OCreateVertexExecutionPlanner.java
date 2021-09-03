package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OCreateVertexStatement;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import java.util.ArrayList;
import java.util.List;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OCreateVertexExecutionPlanner extends OInsertExecutionPlanner {

  public OCreateVertexExecutionPlanner(OCreateVertexStatement statement) {
    this.targetClass =
        statement.getTargetClass() == null ? null : statement.getTargetClass().copy();
    this.targetClusterName =
        statement.getTargetClusterName() == null ? null : statement.getTargetClusterName().copy();
    this.targetCluster =
        statement.getTargetCluster() == null ? null : statement.getTargetCluster().copy();
    if (this.targetClass == null && this.targetCluster == null && this.targetClusterName == null) {
      this.targetClass = new OIdentifier("V");
    }
    this.insertBody = statement.getInsertBody() == null ? null : statement.getInsertBody().copy();
    this.returnStatement =
        statement.getReturnStatement() == null ? null : statement.getReturnStatement().copy();
  }

  @Override
  public OInsertExecutionPlan createExecutionPlan(OCommandContext ctx, boolean enableProfiling) {
    OInsertExecutionPlan prev = super.createExecutionPlan(ctx, enableProfiling);
    List<OExecutionStep> steps = new ArrayList<>(prev.getSteps());
    OInsertExecutionPlan result = new OInsertExecutionPlan(ctx);

    handleCheckType(result, ctx, enableProfiling);
    for (OExecutionStep step : steps) {
      result.chain((OExecutionStepInternal) step);
    }
    return result;
  }

  private void handleCheckType(
      OInsertExecutionPlan result, OCommandContext ctx, boolean profilingEnabled) {
    if (targetClass != null) {
      result.chain(
          new CheckClassTypeStep(targetClass.getStringValue(), "V", ctx, profilingEnabled));
    }
    if (targetClusterName != null) {
      result.chain(
          new CheckClusterTypeStep(targetClusterName.getStringValue(), "V", ctx, profilingEnabled));
    }
    if (targetCluster != null) {
      result.chain(new CheckClusterTypeStep(targetCluster, "V", ctx, profilingEnabled));
    }
  }
}
