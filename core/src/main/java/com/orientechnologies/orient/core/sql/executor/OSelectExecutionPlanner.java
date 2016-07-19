package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.parser.*;

import java.util.ArrayList;

/**
 * Created by luigidellaquila on 06/07/16.
 */
public class OSelectExecutionPlanner {

  private boolean distinct = false;
  private boolean expand   = false;

  private OProjection preAggregateProjection;
  private OProjection aggregateProjection;
  private OProjection projection = null;

  private OLetClause globalLetClause    = null;
  private OLetClause perRecordLetClause = null;
  private OFromClause  target;
  private OWhereClause whereClause;
  private OGroupBy     groupBy;
  private OOrderBy     orderBy;
  private OSkip        skip;
  private OLimit       limit;

  private boolean orderApplied          = false;
  private boolean projectionsCalculated = false;

  public OSelectExecutionPlanner(OSelectStatement oSelectStatement) {
    //copying the content, so that it can be manipulated and optimized
    this.projection = oSelectStatement.getProjection();
    this.target = oSelectStatement.getTarget();
    this.whereClause = oSelectStatement.getWhereClause();
    this.perRecordLetClause = oSelectStatement.getLetClause();
    this.groupBy = oSelectStatement.getGroupBy();
    this.orderBy = oSelectStatement.getOrderBy();
    this.skip = oSelectStatement.getSkip();
    this.limit = oSelectStatement.getLimit();
  }

  public OInternalExecutionPlan createExecutionPlan(OCommandContext ctx) {
    OSelectExecutionPlan result = new OSelectExecutionPlan(ctx);
    optimizeQuery();

    handleGlobalLet(result, globalLetClause, ctx);
    handleFetchFromTarger(result, ctx);
    handleLet(result, perRecordLetClause, ctx);

    handleWhere(result, whereClause, ctx);
    handleExpand(result, ctx);

    handleProjectionsBeforeOrderBy(result, projection, orderBy, ctx);
    handleOrderBy(result, orderBy, ctx);

    if (skip != null) {
      result.chain(new SkipExecutionStep(skip, ctx));
    }

    if (limit != null) {
      result.chain(new LimitExecutionStep(limit, ctx));
    }
    handleProjections(result, ctx);

    return result;
  }

  private void handleProjectionsBeforeOrderBy(OSelectExecutionPlan result, OProjection projection, OOrderBy orderBy,
      OCommandContext ctx) {
    if (orderBy != null) {
      handleProjections(result, ctx);
    }
  }

  private void handleProjections(OSelectExecutionPlan result, OCommandContext ctx) {
    if (!this.projectionsCalculated && projection != null) {
      if (preAggregateProjection != null) {
        result.chain(new ProjectionCalculationStep(preAggregateProjection, ctx));
      }
      if (aggregateProjection != null) {
        result.chain(new AggregateProjectionCalculationStep(aggregateProjection, groupBy, ctx));
      }
      result.chain(new ProjectionCalculationStep(projection, ctx));

      this.projectionsCalculated = true;
    }
  }

  private void optimizeQuery() {
    if (projection != null && this.projection.isExpand()) {
      expand = true;
      this.projection = projection.getExpandContent();
    }

    extractAggregateProjections();
    extractSubQueries();
  }

  private void extractAggregateProjections() {
    if (projection == null) {
      return;
    }

    OProjection preAggregate = new OProjection(-1);
    preAggregate.setItems(new ArrayList<>());
    OProjection aggregate = new OProjection(-1);
    aggregate.setItems(new ArrayList<>());
    OProjection postAggregate = new OProjection(-1);
    postAggregate.setItems(new ArrayList<>());

    boolean isAggregate = false;
    AggregateProjectionSplit result = new AggregateProjectionSplit();
    for (OProjectionItem item : this.projection.getItems()) {
      result.reset();
      if (item.isAggregate()) {
        isAggregate = true;
        OProjectionItem post = item.splitForAggregation(result);
        OIdentifier postAlias = item.getProjectionAlias();
        postAlias.setQuoted(true);
        post.setAlias(postAlias);
        postAggregate.getItems().add(post);
        aggregate.getItems().addAll(result.getAggregate());
        preAggregate.getItems().addAll(result.getPreAggregate());
      } else {
        preAggregate.getItems().add(item);
        //also push the alias forward in the chain
        OProjectionItem aggItem = new OProjectionItem(-1);
        aggItem.setExpression(new OExpression(item.getProjectionAlias()));
        aggregate.getItems().add(aggItem);
        postAggregate.getItems().add(aggItem);
      }
    }
    if (isAggregate) {
      this.preAggregateProjection = preAggregate;
      if (preAggregateProjection.getItems() == null || preAggregateProjection.getItems().size() == 0) {
        preAggregateProjection = null;
      }
      this.aggregateProjection = aggregate;
      if (aggregateProjection.getItems() == null || aggregateProjection.getItems().size() == 0) {
        aggregateProjection = null;
      }
      this.projection = postAggregate;

      addGroupByExpressionsToProjections();
    }
  }

  /**
   * if GROUP BY is performed on an expression that is not explicitly in the pre-aggregate projections, then
   * that expression has to be put in the pre-aggregate (only here, in subsequent steps it's removed)
   */
  private void addGroupByExpressionsToProjections() {
    if (this.groupBy == null || this.groupBy.getItems() == null || this.groupBy.getItems().size() == 0) {
      return;
    }
    OGroupBy newGroupBy = new OGroupBy(-1);
    int i = 0;
    for (OExpression exp : groupBy.getItems()) {
      if (exp.isAggregate()) {
        throw new OCommandExecutionException("Cannot group by an aggregate function");
      }
      boolean found = false;
      for (String alias : preAggregateProjection.getAllAliases()) {
        if (alias.equals(exp.getDefaultAlias().getStringValue())) {
          found = true;
          newGroupBy.getItems().add(exp);
          break;
        }
      }
      if (!found) {
        OProjectionItem newItem = new OProjectionItem(-1);
        newItem.setExpression(exp);
        OIdentifier groupByAlias = new OIdentifier(-1);
        groupByAlias.setStringValue("__$$$GROUP_BY_ALIAS$$$__"+i);
        newItem.setAlias(groupByAlias);
        preAggregateProjection.getItems().add(newItem);
        newGroupBy.getItems().add(new OExpression(groupByAlias));
      }

      groupBy = newGroupBy;
      
      //TODO check ORDER BY and see if that projection has to be also propagated
    }

  }

  /**
   * translates subqueries to LET statements
   */
  private void extractSubQueries() {
    if (whereClause != null && whereClause.containsSubqueries()) {
      //TODO
    }
  }

  private void handleFetchFromTarger(OSelectExecutionPlan result, OCommandContext ctx) {

    if (target == null) {
      result.chain(new ProjectionCalculationNoTargetStep(projection, ctx));
      projectionsCalculated = true;
    } else {
      OFromItem target = this.target.getItem();
      if (target.getIdentifier() != null) {
        handleClassAsTarget(result, target.getIdentifier(), ctx);
      } else {
        throw new UnsupportedOperationException();
      }
    }
  }

  private void handleExpand(OSelectExecutionPlan result, OCommandContext ctx) {
    if (expand) {
      //TODO
    }
  }

  private void handleGlobalLet(OSelectExecutionPlan result, OLetClause letClause, OCommandContext ctx) {
    if (letClause != null) {
      //TODO
    }
  }

  private void handleLet(OSelectExecutionPlan result, OLetClause letClause, OCommandContext ctx) {
    if (letClause != null) {
      //TODO
    }
  }

  private void handleWhere(OSelectExecutionPlan plan, OWhereClause whereClause, OCommandContext ctx) {
    if (whereClause != null) {
      plan.chain(new FilterStep(whereClause, ctx));
    }
  }

  private void handleOrderBy(OSelectExecutionPlan plan, OOrderBy orderBy, OCommandContext ctx) {
    if (!orderApplied && orderBy != null && orderBy.getItems() != null && orderBy.getItems().size() > 0) {
      plan.chain(new OrderByStep(orderBy, ctx));
    }
  }

  private void handleClassAsTarget(OSelectExecutionPlan plan, OIdentifier identifier, OCommandContext ctx) {
    //TODO optimize fetch from class, eg. when you can use and index or when you can do early sort.

    Boolean orderByRidAsc = null;//null: no order. true: asc, false:desc
    if (isOrderByRidAsc()) {
      orderByRidAsc = true;
    } else if (isOrderByRidDesc()) {
      orderByRidAsc = false;
    }
    FetchFromClassExecutionStep fetcher = new FetchFromClassExecutionStep(identifier.getStringValue(), ctx, orderByRidAsc);
    if (orderByRidAsc != null) {
      this.orderApplied = true;
    }
    plan.chain(fetcher);

  }

  private boolean isOrderByRidDesc() {
    if (!hasClassAsTarget()) {
      return false;
    }

    if (orderBy == null) {
      return false;
    }
    if (orderBy.getItems().size() == 1) {
      OOrderByItem item = orderBy.getItems().get(0);
      String recordAttr = item.getRecordAttr();
      if (recordAttr != null && recordAttr.equalsIgnoreCase("@rid") && OOrderByItem.DESC.equals(item.getType())) {
        return true;
      }
    }
    return false;
  }

  private boolean isOrderByRidAsc() {
    if (!hasClassAsTarget()) {
      return false;
    }

    if (orderBy == null) {
      return false;
    }
    if (orderBy.getItems().size() == 1) {
      OOrderByItem item = orderBy.getItems().get(0);
      String recordAttr = item.getRecordAttr();
      if (recordAttr != null && recordAttr.equalsIgnoreCase("@rid") &&
          (item.getType() == null || OOrderByItem.ASC.equals(item.getType()))) {
        return true;
      }
    }
    return false;
  }

  private boolean hasClassAsTarget() {
    if (target == null) {
      return false;
    }
    if (target.getItem() == null) {
      return false;
    }
    if (target.getItem().getIdentifier() != null) {
      return true;
    }
    return false;
  }

}
