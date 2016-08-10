package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.parser.*;
import com.orientechnologies.orient.core.storage.OStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by luigidellaquila on 08/08/16.
 */
public class OUpdateExecutionPlanner {
  private final OFromClause  target;
  public        OWhereClause whereClause;

  protected boolean upsert = false;

  protected List<OUpdateOperations> operations   = new ArrayList<OUpdateOperations>();
  protected boolean                 returnBefore = false;
  protected boolean                 returnAfter  = false;
  protected boolean                 returnCount  = false;

  protected OProjection returnProjection;

  public OStorage.LOCKING_STRATEGY lockRecord = null;

  public OLimit   limit;
  public OTimeout timeout;

  public OUpdateExecutionPlanner(OUpdateStatement oUpdateStatement) {
    this.target = oUpdateStatement.getTarget().copy();
    this.whereClause = oUpdateStatement.getWhereClause() == null ? null : oUpdateStatement.getWhereClause().copy();
    this.operations = oUpdateStatement.getOperations() == null ?
        null :
        oUpdateStatement.getOperations().stream().map(x -> x.copy()).collect(Collectors.toList());
    this.upsert = oUpdateStatement.isUpsert();

    this.returnBefore = oUpdateStatement.isReturnBefore();
    this.returnAfter = oUpdateStatement.isReturnAfter();
    this.returnCount = !(returnAfter || returnBefore);
    this.returnProjection = oUpdateStatement.getReturnProjection() == null ? null : oUpdateStatement.getReturnProjection().copy();
    this.lockRecord = oUpdateStatement.getLockRecord();
    this.limit = oUpdateStatement.getLimit() == null ? null : oUpdateStatement.getLimit().copy();
    this.timeout = oUpdateStatement.getTimeout() == null ? null : oUpdateStatement.getTimeout().copy();
  }

  public OUpdateExecutionPlan createExecutionPlan(OCommandContext ctx) {
    OUpdateExecutionPlan result = new OUpdateExecutionPlan(ctx);

    handleTarget(result, ctx, this.target, this.whereClause, this.timeout);
    handleUpsert(result, ctx, this.target, this.whereClause, this.upsert);
    handleTimeout(result, ctx, this.timeout);
    convertToModifiableResult(result, ctx);
    handleLimit(result, ctx, this.limit);
    handleReturnBefore(result, ctx, this.returnBefore);
    handleOperations(result, ctx, this.operations);
    handleLock(result, ctx, this.lockRecord);
    handleSave(result, ctx);
    handleResultForReturnBefore(result, ctx, this.returnBefore, returnProjection);
    handleResultForReturnAfter(result, ctx, this.returnAfter, returnProjection);
    handleResultForReturnCount(result, ctx, this.returnCount);
    return result;
  }

  /**
   * add a step that transforms a normal OResult in a specific object that under setProperty() updates the actual OIdentifiable
   *
   * @param plan the execution plan
   * @param ctx  the executino context
   */
  private void convertToModifiableResult(OUpdateExecutionPlan plan, OCommandContext ctx) {
    plan.chain(new ConvertToUpdatableResultStep(ctx));
  }

  private void handleResultForReturnCount(OUpdateExecutionPlan result, OCommandContext ctx, boolean returnCount) {
    if (returnCount) {
      result.chain(new CountStep(ctx));
    }
  }

  private void handleResultForReturnAfter(OUpdateExecutionPlan result, OCommandContext ctx, boolean returnAfter,
      OProjection returnProjection) {
    if (returnAfter) {
      //re-convert to normal step
      result.chain(new ConvertToResultInternalStep(ctx));
      if (returnProjection != null) {
        result.chain(new ProjectionCalculationStep(returnProjection, ctx));
      }
    }
  }

  private void handleResultForReturnBefore(OUpdateExecutionPlan result, OCommandContext ctx, boolean returnBefore,
      OProjection returnProjection) {
    if (returnBefore) {
      result.chain(new UnwrapPreviousValueStep(ctx));
      if (returnProjection != null) {
        result.chain(new ProjectionCalculationStep(returnProjection, ctx));
      }
    }
  }

  private void handleSave(OUpdateExecutionPlan result, OCommandContext ctx) {
    result.chain(new SaveElementStep(ctx));
  }

  private void handleTimeout(OUpdateExecutionPlan result, OCommandContext ctx, OTimeout timeout) {
    if (timeout != null && timeout.getVal().longValue() > 0) {
      result.chain(new TimeoutStep(timeout, ctx));
    }
  }

  private void handleReturnBefore(OUpdateExecutionPlan result, OCommandContext ctx, boolean returnBefore) {
    if (returnBefore) {
      result.chain(new CopyRecordContentBeforeUpdateStep(ctx));
    }
  }

  private void handleLock(OUpdateExecutionPlan result, OCommandContext ctx, OStorage.LOCKING_STRATEGY lockRecord) {

  }

  private void handleLimit(OUpdateExecutionPlan plan, OCommandContext ctx, OLimit limit) {
    if (limit != null) {
      plan.chain(new LimitExecutionStep(limit, ctx));
    }
  }

  private void handleUpsert(OUpdateExecutionPlan plan, OCommandContext ctx, OFromClause target, OWhereClause where,
      boolean upsert) {
    if (upsert) {
      plan.chain(new UpsertStep(target, where, ctx));
    }
  }

  private void handleOperations(OUpdateExecutionPlan plan, OCommandContext ctx, List<OUpdateOperations> ops) {
    if (ops != null) {
      for (OUpdateOperations op : ops) {
        switch (op.getType()) {
        case OUpdateOperations.TYPE_SET:
          plan.chain(new UpdateSetStep(op.getUpdateItems(), ctx));
          break;
        case OUpdateOperations.TYPE_REMOVE:
          plan.chain(new UpdateRemoveStep(op.getUpdateRemoveItems(), ctx));
          break;
        case OUpdateOperations.TYPE_MERGE:
          plan.chain(new UpdateMergeStep(op.getJson(), ctx));
          break;
        case OUpdateOperations.TYPE_CONTENT:
          plan.chain(new UpdateContentStep(op.getJson(), ctx));
          break;
        case OUpdateOperations.TYPE_PUT:
        case OUpdateOperations.TYPE_INCREMENT:
        case OUpdateOperations.TYPE_ADD:
          throw new OCommandExecutionException("Cannot execute with UPDATE PUT/ADD/INCREMENT new executor: " + op);
        }
      }
    }
  }

  private void handleTarget(OUpdateExecutionPlan result, OCommandContext ctx, OFromClause target, OWhereClause whereClause,
      OTimeout timeout) {
    OSelectStatement sourceStatement = new OSelectStatement(-1);
    sourceStatement.setTarget(target);
    sourceStatement.setWhereClause(whereClause);
    if (timeout != null) {
      sourceStatement.setTimeout(this.timeout.copy());
    }
    OSelectExecutionPlanner planner = new OSelectExecutionPlanner(sourceStatement);
    result.chain(new SubQueryStep(planner.createExecutionPlan(ctx), ctx, ctx));
  }
}