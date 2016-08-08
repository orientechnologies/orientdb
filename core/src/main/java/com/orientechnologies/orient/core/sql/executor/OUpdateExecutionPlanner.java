package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
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
    this.returnCount = oUpdateStatement.isReturnCount();
    this.returnProjection = oUpdateStatement.getReturnProjection() == null ? null : oUpdateStatement.getReturnProjection().copy();
    this.lockRecord = oUpdateStatement.getLockRecord();
    this.limit = oUpdateStatement.getLimit() == null ? null : oUpdateStatement.getLimit().copy();
    this.timeout = oUpdateStatement.getTimeout() == null ? null : oUpdateStatement.getTimeout().copy();
  }

  public OUpdateExecutionPlan createExecutionPlan(OCommandContext ctx) {
    OUpdateExecutionPlan result = new OUpdateExecutionPlan(ctx);

    handleTarget(result, ctx, this.target, this.whereClause, this.timeout);
    handleTimeout(result, ctx, this.timeout);
    convertToModifiableResult(result, ctx);
    handleUpsert(result, ctx, this.target, this.whereClause, result);
    handleLimit(result, ctx, this.limit);
    handleReturnBefore(result, ctx, this.returnBefore);
    handleOperations(result, ctx, this.operations);
    handleLock(result, ctx, this.lockRecord);
    handleSave(result, ctx, this.returnBefore);
    handleResultForReturnBefore(result, ctx, this.returnBefore);
    handleResultForReturnAfter(result, ctx, this.returnAfter);
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
    //TODO
  }

  private void handleResultForReturnCount(OUpdateExecutionPlan result, OCommandContext ctx, boolean returnCount) {
    //TODO
  }

  private void handleResultForReturnAfter(OUpdateExecutionPlan result, OCommandContext ctx, boolean returnAfter) {
    //TODO
  }

  private void handleResultForReturnBefore(OUpdateExecutionPlan result, OCommandContext ctx, boolean returnBefore) {
    //TODO
  }

  private void handleSave(OUpdateExecutionPlan result, OCommandContext ctx, boolean returnBefore) {
    //TODO
  }

  private void handleTimeout(OUpdateExecutionPlan result, OCommandContext ctx, OTimeout timeout) {
    if (timeout != null && timeout.getVal().longValue() > 0) {
      result.chain(new TimeoutStep(timeout, ctx));
    }
  }

  private void handleReturnBefore(OUpdateExecutionPlan result, OCommandContext ctx, boolean returnBefore) {
    //TODO
  }

  private void handleLock(OUpdateExecutionPlan result, OCommandContext ctx, OStorage.LOCKING_STRATEGY lockRecord) {

  }

  private void handleLimit(OUpdateExecutionPlan plan, OCommandContext ctx, OLimit limit) {
    if (limit != null) {
      plan.chain(new LimitExecutionStep(limit, ctx));
    }
  }

  private void handleUpsert(OUpdateExecutionPlan plan, OCommandContext ctx, OFromClause target, OWhereClause where,
      OUpdateExecutionPlan result) {
    //TODO
  }

  private void handleOperations(OUpdateExecutionPlan plan, OCommandContext ctx, List<OUpdateOperations> ops) {
    //TODO
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