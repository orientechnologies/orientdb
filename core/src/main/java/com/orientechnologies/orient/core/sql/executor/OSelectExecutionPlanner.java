package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.*;

import java.util.Set;

/**
 * Created by luigidellaquila on 06/07/16.
 */
public class OSelectExecutionPlanner {

  private boolean     distinct   = false;
  private boolean     expand     = false;
  private OProjection projection = null;
  private final OLetClause   letClause;
  private OFromClause target;
  private final OWhereClause whereClause;
  private final OOrderBy orderBy;
  private final OSkip skip;
  private final OLimit limit;

  private boolean orderApplied          = false;
  private boolean projectionsCalculated = false;

  public OSelectExecutionPlanner(OSelectStatement oSelectStatement) {
    //copying the content, so that it can be manipulated and optimized
    this.projection = oSelectStatement.getProjection();
    this.target = oSelectStatement.getTarget();
    this.whereClause = oSelectStatement.getWhereClause();
    this.letClause = oSelectStatement.getLetClause();
    this.orderBy = oSelectStatement.getOrderBy();
    this.skip = oSelectStatement.getSkip();
    this.limit = oSelectStatement.getLimit();
  }

  public OExecutionPlan createExecutionPlan(OCommandContext ctx) {
    OSelectExecutionPlan result = new OSelectExecutionPlan(ctx);
    optimizeQuery();

    handleFetchFromTarger(result, ctx);
    handleLet(result, letClause, ctx);
    handleProjectionsBeforeWhere(result, projection, whereClause, ctx);
    handleWhere(result, whereClause, ctx);
    handleExpand(result, ctx);

    if (orderBy != null) {
      handleOrderBy(result, orderBy, ctx);
    }

    if (skip != null) {
      result.chain(new SkipExecutionStep(skip, ctx));
    }

    if (limit != null) {
      result.chain(new LimitExecutionStep(limit, ctx));
    }
    handleProjections(result, projection, ctx);

    return result;
  }

  private void handleProjections(OSelectExecutionPlan result, OProjection projection, OCommandContext ctx) {
    if (!this.projectionsCalculated) {
      result.chain(new ProjectionCalculationStep(projection, ctx));
    }
  }

  private void optimizeQuery() {
    if (projection != null && this.projection.isExpand()) {
      expand = true;
      this.projection = projection.getExpandContent();
    }
  }

  private void handleFetchFromTarger(OSelectExecutionPlan result, OCommandContext ctx) {

    if (target == null) {
      result.chain(new NoTargetProjectionEvaluator(projection, ctx));
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

  private void handleProjectionsBeforeWhere(OSelectExecutionPlan result, OProjection projection, OWhereClause whereClause,
      OCommandContext ctx) {
    if (whereClause != null && projection != null) {
      Set<String> aliases = projection.getAllAliases();
      if (whereClause.needsAliases(aliases)) {
        //TODO
      }
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
    FetchFromClassExecutionStep fetcher = new FetchFromClassExecutionStep(identifier.getStringValue(), ctx,
        orderByRidAsc);
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
