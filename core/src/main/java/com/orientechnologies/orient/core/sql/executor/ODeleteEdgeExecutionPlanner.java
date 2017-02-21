package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.sql.parser.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by luigidellaquila on 08/08/16.
 */
public class ODeleteEdgeExecutionPlanner {

  protected OIdentifier className;
  protected OIdentifier targetClusterName;

  protected List<ORid> rids;

  private OExpression leftExpression;
  private OExpression rightExpression;

  protected OBatch batch = null;

  private OWhereClause whereClause;

  private OLimit limit;

  public ODeleteEdgeExecutionPlanner(ODeleteEdgeStatement stm) {

    this.className = stm.getClassName() == null ? null : stm.getClassName().copy();
    this.targetClusterName = stm.getTargetClusterName() == null ? null : stm.getTargetClusterName().copy();
    if (stm.getRid() != null) {
      this.rids = new ArrayList<>();
      rids.add(stm.getRid().copy());
    } else {
      this.rids = stm.getRids() == null ? null : stm.getRids().stream().map(x -> x.copy()).collect(Collectors.toList());
    }

    this.leftExpression = stm.getLeftExpression() == null ? null : stm.getLeftExpression().copy();
    this.rightExpression = stm.getRightExpression() == null ? null : stm.getRightExpression().copy();

    this.whereClause = stm.getWhereClause() == null ? null : stm.getWhereClause().copy();
    this.batch = stm.getBatch() == null ? null : stm.getBatch().copy();
    this.limit = stm.getLimit() == null ? null : stm.getLimit().copy();
  }

  public ODeleteExecutionPlan createExecutionPlan(OCommandContext ctx) {
    ODeleteExecutionPlan result = new ODeleteExecutionPlan(ctx);

    if (leftExpression != null || rightExpression != null) {
      handleGlobalLet(result, new OIdentifier("$__ORIENT_DELETE_EDGE_fromV"), leftExpression, ctx);
      handleGlobalLet(result, new OIdentifier("$__ORIENT_DELETE_EDGE_toV"), rightExpression, ctx);
      handleFetchFromTo(result, ctx, "$__ORIENT_DELETE_EDGE_fromV", "$__ORIENT_DELETE_EDGE_toV", className, targetClusterName);
      handleWhere(result, ctx, whereClause);
    } else if (whereClause != null) {
      OFromClause fromClause = new OFromClause(-1);
      OFromItem item = new OFromItem(-1);
      item.setIdentifier(className);
      fromClause.setItem(item);
      handleTarget(result, ctx, fromClause, this.whereClause);
    } else {
      handleTargetClass(result, ctx, className);
      handleTargetCluster(result, ctx, targetClusterName);
      handleTargetRids(result, ctx, rids);
    }

    handleLimit(result, ctx, this.limit);

    handleCastToEdge(result, ctx);

    handleDelete(result, ctx);

    handleReturn(result, ctx);
    return result;
  }

  private void handleWhere(ODeleteExecutionPlan result, OCommandContext ctx, OWhereClause whereClause) {
    if (whereClause != null) {
      result.chain(new FilterStep(whereClause, ctx));
    }
  }

  private void handleFetchFromTo(ODeleteExecutionPlan result, OCommandContext ctx, String fromAlias, String toAlias,
      OIdentifier targetClass, OIdentifier targetCluster) {
    if (fromAlias != null && toAlias != null) {
      result.chain(new FetchEdgesFromToVerticesStep(fromAlias, toAlias, targetClass, targetCluster, ctx));
    } else if (toAlias != null) {
      result.chain(new FetchEdgesToVerticesStep(toAlias, targetClass, targetCluster, ctx));
    }
  }

  private void handleTargetRids(ODeleteExecutionPlan result, OCommandContext ctx, List<ORid> rids) {
    if (rids != null) {
      result.chain(new FetchFromRidsStep(rids.stream().map(x -> x.toRecordId()).collect(Collectors.toList()), ctx));
    }
  }

  private void handleTargetCluster(ODeleteExecutionPlan result, OCommandContext ctx, OIdentifier targetClusterName) {
    if (targetClusterName != null) {
      String name = targetClusterName.getStringValue();
      int clusterId = ctx.getDatabase().getClusterIdByName(name);
      if (clusterId < 0) {
        throw new OCommandExecutionException("Cluster not found: " + name);
      }
      result.chain(new FetchFromClusterExecutionStep(clusterId, ctx));
    }
  }

  private void handleTargetClass(ODeleteExecutionPlan result, OCommandContext ctx, OIdentifier className) {
    if (className != null) {
      result.chain(new FetchFromClassExecutionStep(className.getStringValue(), ctx, null));
    }
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

  private void handleReturn(ODeleteExecutionPlan result, OCommandContext ctx) {
    result.chain(new CountStep(ctx));
  }

  private void handleLimit(OUpdateExecutionPlan plan, OCommandContext ctx, OLimit limit) {
    if (limit != null) {
      plan.chain(new LimitExecutionStep(limit, ctx));
    }
  }

  private void handleCastToEdge(ODeleteExecutionPlan plan, OCommandContext ctx) {
    plan.chain(new CastToEdgeStep(ctx));
  }

  private void handleTarget(OUpdateExecutionPlan result, OCommandContext ctx, OFromClause target, OWhereClause whereClause) {
    OSelectStatement sourceStatement = new OSelectStatement(-1);
    sourceStatement.setTarget(target);
    sourceStatement.setWhereClause(whereClause);
    OSelectExecutionPlanner planner = new OSelectExecutionPlanner(sourceStatement);
    result.chain(new SubQueryStep(planner.createExecutionPlan(ctx), ctx, ctx));
  }

  private void handleGlobalLet(ODeleteExecutionPlan result, OIdentifier name, OExpression expression, OCommandContext ctx) {
    result.chain(new GlobalLetExpressionStep(name, expression, ctx));
  }
}
