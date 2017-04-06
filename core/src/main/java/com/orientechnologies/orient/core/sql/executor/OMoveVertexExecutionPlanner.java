package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.parser.*;
import com.orientechnologies.orient.core.storage.OStorage;

/**
 * Created by luigidellaquila on 08/08/16.
 */
public class OMoveVertexExecutionPlanner {
  private final OFromItem         source;
  private final OIdentifier       targetClass;
  private final OCluster          targetCluster;
  private final OUpdateOperations updateOperations;
  private final OBatch            batch;

  public OMoveVertexExecutionPlanner(OMoveVertexStatement oStatement) {
    this.source = oStatement.getSource();
    this.targetClass = oStatement.getTargetClass();
    this.targetCluster = oStatement.getTargetCluster();
    this.updateOperations = oStatement.getUpdateOperations();
    this.batch = oStatement.getBatch();
  }

  public OUpdateExecutionPlan createExecutionPlan(OCommandContext ctx, boolean enableProfiling) {
    OUpdateExecutionPlan result = new OUpdateExecutionPlan(ctx);

    handleSource(result, ctx, this.source, enableProfiling);
    convertToModifiableResult(result, ctx, enableProfiling);
    handleTarget(result, targetClass, targetCluster, ctx, enableProfiling);
    handleOperations(result, ctx, this.updateOperations, enableProfiling);
    handleBatch(result, ctx, this.batch, enableProfiling);
    handleSave(result, ctx, enableProfiling);
    return result;
  }

  private void handleTarget(OUpdateExecutionPlan result, OIdentifier targetClass, OCluster targetCluster, OCommandContext ctx, boolean profilingEnabled) {
    result.chain(new MoveVertexStep(targetClass, targetCluster, ctx, profilingEnabled));
  }

  private void handleBatch(OUpdateExecutionPlan result, OCommandContext ctx, OBatch batch, boolean profilingEnabled) {
    if (batch != null) {
      result.chain(new BatchStep(batch, ctx, profilingEnabled));
    }
  }

  /**
   * add a step that transforms a normal OResult in a specific object that under setProperty() updates the actual OIdentifiable
   *
   * @param plan the execution plan
   * @param ctx  the executino context
   */
  private void convertToModifiableResult(OUpdateExecutionPlan plan, OCommandContext ctx, boolean profilingEnabled) {
    plan.chain(new ConvertToUpdatableResultStep(ctx, profilingEnabled));
  }

  private void handleResultForReturnCount(OUpdateExecutionPlan result, OCommandContext ctx, boolean returnCount, boolean profilingEnabled) {
    if (returnCount) {
      result.chain(new CountStep(ctx, profilingEnabled));
    }
  }

  private void handleResultForReturnAfter(OUpdateExecutionPlan result, OCommandContext ctx, boolean returnAfter,
      OProjection returnProjection, boolean profilingEnabled) {
    if (returnAfter) {
      //re-convert to normal step
      result.chain(new ConvertToResultInternalStep(ctx, profilingEnabled));
      if (returnProjection != null) {
        result.chain(new ProjectionCalculationStep(returnProjection, ctx, profilingEnabled));
      }
    }
  }

  private void handleResultForReturnBefore(OUpdateExecutionPlan result, OCommandContext ctx, boolean returnBefore,
      OProjection returnProjection, boolean profilingEnabled) {
    if (returnBefore) {
      result.chain(new UnwrapPreviousValueStep(ctx, profilingEnabled));
      if (returnProjection != null) {
        result.chain(new ProjectionCalculationStep(returnProjection, ctx, profilingEnabled));
      }
    }
  }

  private void handleSave(OUpdateExecutionPlan result, OCommandContext ctx, boolean profilingEnabled) {
    result.chain(new SaveElementStep(ctx, profilingEnabled));
  }

  private void handleTimeout(OUpdateExecutionPlan result, OCommandContext ctx, OTimeout timeout, boolean profilingEnabled) {
    if (timeout != null && timeout.getVal().longValue() > 0) {
      result.chain(new TimeoutStep(timeout, ctx, profilingEnabled));
    }
  }

  private void handleReturnBefore(OUpdateExecutionPlan result, OCommandContext ctx, boolean returnBefore, boolean profilingEnabled) {
    if (returnBefore) {
      result.chain(new CopyRecordContentBeforeUpdateStep(ctx, profilingEnabled));
    }
  }

  private void handleLock(OUpdateExecutionPlan result, OCommandContext ctx, OStorage.LOCKING_STRATEGY lockRecord) {

  }

  private void handleLimit(OUpdateExecutionPlan plan, OCommandContext ctx, OLimit limit, boolean profilingEnabled) {
    if (limit != null) {
      plan.chain(new LimitExecutionStep(limit, ctx, profilingEnabled));
    }
  }

  private void handleUpsert(OUpdateExecutionPlan plan, OCommandContext ctx, OFromClause target, OWhereClause where,
      boolean upsert, boolean profilingEnabled) {
    if (upsert) {
      plan.chain(new UpsertStep(target, where, ctx, profilingEnabled));
    }
  }

  private void handleOperations(OUpdateExecutionPlan plan, OCommandContext ctx, OUpdateOperations op, boolean profilingEnabled) {
    if (op != null) {
      switch (op.getType()) {
      case OUpdateOperations.TYPE_SET:
        plan.chain(new UpdateSetStep(op.getUpdateItems(), ctx, profilingEnabled));
        break;
      case OUpdateOperations.TYPE_REMOVE:
        plan.chain(new UpdateRemoveStep(op.getUpdateRemoveItems(), ctx, profilingEnabled));
        break;
      case OUpdateOperations.TYPE_MERGE:
        plan.chain(new UpdateMergeStep(op.getJson(), ctx, profilingEnabled));
        break;
      case OUpdateOperations.TYPE_CONTENT:
        plan.chain(new UpdateContentStep(op.getJson(), ctx, profilingEnabled));
        break;
      case OUpdateOperations.TYPE_PUT:
      case OUpdateOperations.TYPE_INCREMENT:
      case OUpdateOperations.TYPE_ADD:
        throw new OCommandExecutionException("Cannot execute with UPDATE PUT/ADD/INCREMENT new executor: " + op);
      }
    }
  }

  private void handleSource(OUpdateExecutionPlan result, OCommandContext ctx, OFromItem source, boolean profilingEnabled) {
    OSelectStatement sourceStatement = new OSelectStatement(-1);
    sourceStatement.setTarget(new OFromClause(-1));
    sourceStatement.getTarget().setItem(source);
    OSelectExecutionPlanner planner = new OSelectExecutionPlanner(sourceStatement);
    result.chain(new SubQueryStep(planner.createExecutionPlan(ctx, profilingEnabled), ctx, ctx, profilingEnabled));
  }
}