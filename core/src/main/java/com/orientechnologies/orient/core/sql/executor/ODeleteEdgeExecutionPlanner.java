package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
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

  public ODeleteExecutionPlan createExecutionPlan(OCommandContext ctx, boolean enableProfiling) {
    ODeleteExecutionPlan result = new ODeleteExecutionPlan(ctx);

    if (leftExpression != null || rightExpression != null) {
      handleGlobalLet(result, new OIdentifier("$__ORIENT_DELETE_EDGE_fromV"), leftExpression, ctx, enableProfiling);
      handleGlobalLet(result, new OIdentifier("$__ORIENT_DELETE_EDGE_toV"), rightExpression, ctx, enableProfiling);
      handleFetchFromTo(result, ctx, "$__ORIENT_DELETE_EDGE_fromV", "$__ORIENT_DELETE_EDGE_toV", className, targetClusterName,
          enableProfiling);
      handleWhere(result, ctx, whereClause, enableProfiling);
    } else if (whereClause != null) {
      OFromClause fromClause = new OFromClause(-1);
      OFromItem item = new OFromItem(-1);
      if (className == null) {
        item.setIdentifier(new OIdentifier("E"));
      } else {
        item.setIdentifier(className);
      }
      fromClause.setItem(item);
      handleTarget(result, ctx, fromClause, this.whereClause, enableProfiling);
    } else {
      handleTargetClass(result, ctx, className, enableProfiling);
      handleTargetCluster(result, ctx, targetClusterName, enableProfiling);
      handleTargetRids(result, ctx, rids, enableProfiling);
    }

    handleLimit(result, ctx, this.limit, enableProfiling);

    handleCastToEdge(result, ctx, enableProfiling);

    handleDelete(result, ctx, enableProfiling);

    handleReturn(result, ctx, enableProfiling);
    return result;
  }

  private void handleWhere(ODeleteExecutionPlan result, OCommandContext ctx, OWhereClause whereClause, boolean profilingEnabled) {
    if (whereClause != null) {
      result.chain(new FilterStep(whereClause, ctx, profilingEnabled));
    }
  }

  private void handleFetchFromTo(ODeleteExecutionPlan result, OCommandContext ctx, String fromAlias, String toAlias,
      OIdentifier targetClass, OIdentifier targetCluster, boolean profilingEnabled) {
    if (fromAlias != null && toAlias != null) {
      result.chain(new FetchEdgesFromToVerticesStep(fromAlias, toAlias, targetClass, targetCluster, ctx, profilingEnabled));
    } else if (toAlias != null) {
      result.chain(new FetchEdgesToVerticesStep(toAlias, targetClass, targetCluster, ctx, profilingEnabled));
    }
  }

  private void handleTargetRids(ODeleteExecutionPlan result, OCommandContext ctx, List<ORid> rids, boolean profilingEnabled) {
    if (rids != null) {
      result.chain(
          new FetchFromRidsStep(rids.stream().map(x -> x.toRecordId((OResult) null, ctx)).collect(Collectors.toList()), ctx,
              profilingEnabled));
    }
  }

  private void handleTargetCluster(ODeleteExecutionPlan result, OCommandContext ctx, OIdentifier targetClusterName,
      boolean profilingEnabled) {
    if (targetClusterName != null) {
      String name = targetClusterName.getStringValue();
      int clusterId = ctx.getDatabase().getClusterIdByName(name);
      if (clusterId < 0) {
        throw new OCommandExecutionException("Cluster not found: " + name);
      }
      result.chain(new FetchFromClusterExecutionStep(clusterId, ctx, profilingEnabled));
    }
  }

  private void handleTargetClass(ODeleteExecutionPlan result, OCommandContext ctx, OIdentifier className,
      boolean profilingEnabled) {
    if (className != null) {
      result.chain(new FetchFromClassExecutionStep(className.getStringValue(), null, ctx, null, profilingEnabled));
    }
  }

  private boolean handleIndexAsTarget(ODeleteExecutionPlan result, OIndexIdentifier indexIdentifier, OWhereClause whereClause,
      OCommandContext ctx, boolean profilingEnabled) {
    if (indexIdentifier == null) {
      return false;
    }
    throw new OCommandExecutionException("DELETE VERTEX FROM INDEX is not supported");
  }

  private void handleDelete(ODeleteExecutionPlan result, OCommandContext ctx, boolean profilingEnabled) {
    result.chain(new DeleteStep(ctx, profilingEnabled));
  }

  private void handleReturn(ODeleteExecutionPlan result, OCommandContext ctx, boolean profilingEnabled) {
    result.chain(new CountStep(ctx, profilingEnabled));
  }

  private void handleLimit(OUpdateExecutionPlan plan, OCommandContext ctx, OLimit limit, boolean profilingEnabled) {
    if (limit != null) {
      plan.chain(new LimitExecutionStep(limit, ctx, profilingEnabled));
    }
  }

  private void handleCastToEdge(ODeleteExecutionPlan plan, OCommandContext ctx, boolean profilingEnabled) {
    plan.chain(new CastToEdgeStep(ctx, profilingEnabled));
  }

  private void handleTarget(OUpdateExecutionPlan result, OCommandContext ctx, OFromClause target, OWhereClause whereClause,
      boolean profilingEnabled) {
    OSelectStatement sourceStatement = new OSelectStatement(-1);
    sourceStatement.setTarget(target);
    sourceStatement.setWhereClause(whereClause);
    OSelectExecutionPlanner planner = new OSelectExecutionPlanner(sourceStatement);
    result.chain(new SubQueryStep(planner.createExecutionPlan(ctx, profilingEnabled), ctx, ctx, profilingEnabled));
  }

  private void handleGlobalLet(ODeleteExecutionPlan result, OIdentifier name, OExpression expression, OCommandContext ctx,
      boolean profilingEnabled) {
    if (expression != null) {
      result.chain(new GlobalLetExpressionStep(name, expression, ctx, profilingEnabled));
    }
  }
}
