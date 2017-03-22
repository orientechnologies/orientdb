package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.parser.*;

/**
 * Created by luigidellaquila on 08/08/16.
 */
public class ODeleteVertexExecutionPlanner {

  private final OFromClause  fromClause;
  private final OWhereClause whereClause;
  private final boolean      returnBefore;
  private final OLimit       limit;

  public ODeleteVertexExecutionPlanner(ODeleteVertexStatement stm) {
    this.fromClause = stm.getFromClause() == null ? null : stm.getFromClause().copy();
    this.whereClause = stm.getWhereClause() == null ? null : stm.getWhereClause().copy();
    this.returnBefore = stm.isReturnBefore();
    this.limit = stm.getLimit() == null ? null : stm.getLimit();
  }

  public ODeleteExecutionPlan createExecutionPlan(OCommandContext ctx) {
    ODeleteExecutionPlan result = new ODeleteExecutionPlan(ctx);

    if (handleIndexAsTarget(result, fromClause.getItem().getIndex(), whereClause, ctx)) {
      if (limit != null) {
        throw new OCommandExecutionException("Cannot apply a LIMIT on a delete from index");
      }
      if (returnBefore) {
        throw new OCommandExecutionException("Cannot apply a RETURN BEFORE on a delete from index");
      }

    } else {
      handleTarget(result, ctx, this.fromClause, this.whereClause);
      handleLimit(result, ctx, this.limit);
    }
    handleCastToVertex(result, ctx);
    handleDelete(result, ctx);
    handleReturn(result, ctx, this.returnBefore);
    return result;
  }

  private boolean handleIndexAsTarget(ODeleteExecutionPlan result, OIndexIdentifier indexIdentifier, OWhereClause whereClause,
      OCommandContext ctx) {
    if (indexIdentifier == null) {
      return false;
    }
    throw new OCommandExecutionException("DELETE VERTEX FROM INDEX is not supported");
  }

  private void handleDelete(ODeleteExecutionPlan result, OCommandContext ctx) {
    result.chain(new DeleteStep(ctx));
  }

  private void handleUnsafe(ODeleteExecutionPlan result, OCommandContext ctx, boolean unsafe) {
    if (!unsafe) {
      result.chain(new CheckSafeDeleteStep(ctx));
    }
  }

  private void handleReturn(ODeleteExecutionPlan result, OCommandContext ctx, boolean returnBefore) {
    if (!returnBefore) {
      result.chain(new CountStep(ctx));
    }
  }

  private void handleLimit(OUpdateExecutionPlan plan, OCommandContext ctx, OLimit limit) {
    if (limit != null) {
      plan.chain(new LimitExecutionStep(limit, ctx));
    }
  }

  private void handleCastToVertex(ODeleteExecutionPlan plan, OCommandContext ctx) {
    plan.chain(new CastToVertexStep(ctx));
  }

  private void handleTarget(OUpdateExecutionPlan result, OCommandContext ctx, OFromClause target, OWhereClause whereClause) {
    OSelectStatement sourceStatement = new OSelectStatement(-1);
    sourceStatement.setTarget(target);
    sourceStatement.setWhereClause(whereClause);
    OSelectExecutionPlanner planner = new OSelectExecutionPlanner(sourceStatement);
    result.chain(new SubQueryStep(planner.createExecutionPlan(ctx), ctx, ctx));
  }
}
