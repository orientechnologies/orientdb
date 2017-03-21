package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.sql.parser.*;

import java.util.List;

/**
 * Created by luigidellaquila on 08/08/16.
 */
public class ODeleteExecutionPlanner {

  private final OFromClause  fromClause;
  private final OWhereClause whereClause;
  private final boolean      returnBefore;
  private final OLimit       limit;
  private final boolean      unsafe;

  public ODeleteExecutionPlanner(ODeleteStatement stm) {
    this.fromClause = stm.getFromClause() == null ? null : stm.getFromClause().copy();
    this.whereClause = stm.getWhereClause() == null ? null : stm.getWhereClause().copy();
    this.returnBefore = stm.isReturnBefore();
    this.limit = stm.getLimit() == null ? null : stm.getLimit();
    this.unsafe = stm.isUnsafe();
  }

  public ODeleteExecutionPlan createExecutionPlan(OCommandContext ctx) {
    ODeleteExecutionPlan result = new ODeleteExecutionPlan(ctx);

    if (handleIndexAsTarget(result, fromClause.getItem().getIndex(), whereClause, ctx)) {
      if (limit != null) {
        throw new OCommandExecutionException("Cannot apply a LIMIT on a delete from index");
      }
      if (unsafe) {
        throw new OCommandExecutionException("Cannot apply a UNSAFE on a delete from index");
      }
      if (returnBefore) {
        throw new OCommandExecutionException("Cannot apply a RETURN BEFORE on a delete from index");
      }

      handleReturn(result, ctx, this.returnBefore);
    } else {
      handleTarget(result, ctx, this.fromClause, this.whereClause);
      handleUnsafe(result, ctx, this.unsafe);
      handleLimit(result, ctx, this.limit);
      handleDelete(result, ctx);
      handleReturn(result, ctx, this.returnBefore);
    }
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
      OBooleanExpression keyCondition = null;
      OBooleanExpression ridCondition = null;
      if (flattenedWhereClause == null || flattenedWhereClause.size() == 0) {
        if (!index.supportsOrderedIterations()) {
          throw new OCommandExecutionException("Index " + indexName + " does not allow iteration without a condition");
        }
      } else if (flattenedWhereClause.size() > 1) {
        throw new OCommandExecutionException("Index queries with this kind of condition are not supported yet: " + whereClause);
      } else {
        OAndBlock andBlock = flattenedWhereClause.get(0);
        if (andBlock.getSubBlocks().size() == 1) {

          whereClause = null;//The WHERE clause won't be used anymore, the index does all the filtering
          flattenedWhereClause = null;
          keyCondition = getKeyCondition(andBlock);
          if (keyCondition == null) {
            throw new OCommandExecutionException("Index queries with this kind of condition are not supported yet: " + whereClause);
          }
        } else if (andBlock.getSubBlocks().size() == 2) {
          whereClause = null;//The WHERE clause won't be used anymore, the index does all the filtering
          flattenedWhereClause = null;
          keyCondition = getKeyCondition(andBlock);
          ridCondition = getRidCondition(andBlock);
          if (keyCondition == null || ridCondition == null) {
            throw new OCommandExecutionException("Index queries with this kind of condition are not supported yet: " + whereClause);
          }
        } else {
          throw new OCommandExecutionException("Index queries with this kind of condition are not supported yet: " + whereClause);
        }
      }
      result.chain(new DeleteFromIndexStep(index, keyCondition, null, ridCondition, ctx));
      if (ridCondition != null) {
        OWhereClause where = new OWhereClause(-1);
        where.setBaseExpression(ridCondition);
        result.chain(new FilterStep(where, ctx));
      }
      return true;
    case VALUES:
    case VALUESASC:
      if (!index.supportsOrderedIterations()) {
        throw new OCommandExecutionException("Index " + indexName + " does not allow iteration on values");
      }
      result.chain(new FetchFromIndexValuesStep(index, true, ctx));
      result.chain(new GetValueFromIndexEntryStep(ctx));
      break;
    case VALUESDESC:
      if (!index.supportsOrderedIterations()) {
        throw new OCommandExecutionException("Index " + indexName + " does not allow iteration on values");
      }
      result.chain(new FetchFromIndexValuesStep(index, false, ctx));
      result.chain(new GetValueFromIndexEntryStep(ctx));
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

  private void handleTarget(OUpdateExecutionPlan result, OCommandContext ctx, OFromClause target, OWhereClause whereClause) {
    OSelectStatement sourceStatement = new OSelectStatement(-1);
    sourceStatement.setTarget(target);
    sourceStatement.setWhereClause(whereClause);
    OSelectExecutionPlanner planner = new OSelectExecutionPlanner(sourceStatement);
    result.chain(new SubQueryStep(planner.createExecutionPlan(ctx), ctx, ctx));
  }

  private OBooleanExpression getKeyCondition(OAndBlock andBlock) {
    for (OBooleanExpression exp : andBlock.getSubBlocks()) {
      String str = exp.toString();
      if (str.length() < 5) {
        continue;
      }
      if (str.substring(0, 4).equalsIgnoreCase("key ")) {
        return exp;
      }
    }
    return null;
  }

  private OBooleanExpression getRidCondition(OAndBlock andBlock) {
    for (OBooleanExpression exp : andBlock.getSubBlocks()) {
      String str = exp.toString();
      if (str.length() < 5) {
        continue;
      }
      if (str.substring(0, 4).equalsIgnoreCase("rid ")) {
        return exp;
      }
    }
    return null;
  }

}
