package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.sql.parser.*;

import java.util.List;

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
    String indexName = indexIdentifier.getIndexName();
    OIndex<?> index = ctx.getDatabase().getMetadata().getIndexManager().getIndex(indexName);
    if (index == null) {
      throw new OCommandExecutionException("Index not found: " + indexName);
    }
    List<OAndBlock> flattenedWhereClause = whereClause == null ? null : whereClause.flatten();

    switch (indexIdentifier.getType()) {
    case INDEX:
      OBooleanExpression condition = null;
      if (flattenedWhereClause == null || flattenedWhereClause.size() == 0) {
        if (!index.supportsOrderedIterations()) {
          throw new OCommandExecutionException("Index " + indexName + " does not allow iteration without a condition");
        }
      } else if (flattenedWhereClause.size() > 1) {
        throw new OCommandExecutionException("Index queries with this kind of condition are not supported yet: " + whereClause);
      } else {
        OAndBlock andBlock = flattenedWhereClause.get(0);
        if (andBlock.getSubBlocks().size() != 1) {
          throw new OCommandExecutionException("Index queries with this kind of condition are not supported yet: " + whereClause);
        }

        condition = andBlock.getSubBlocks().get(0);
      }
      result.chain(new DeleteFromIndexStep(index, condition, null, ctx));
      return true;
    case VALUES:
    case VALUESASC:
      if (!index.supportsOrderedIterations()) {
        throw new OCommandExecutionException("Index " + indexName + " does not allow iteration on values");
      }
      result.chain(new FetchFromIndexValuesStep(index, true, ctx));
      break;
    case VALUESDESC:
      if (!index.supportsOrderedIterations()) {
        throw new OCommandExecutionException("Index " + indexName + " does not allow iteration on values");
      }
      result.chain(new FetchFromIndexValuesStep(index, false, ctx));
      break;
    }
    return false;
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
