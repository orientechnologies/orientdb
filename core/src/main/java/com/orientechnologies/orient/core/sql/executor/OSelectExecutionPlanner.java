package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.*;

import java.util.Set;

/**
 * Created by luigidellaquila on 06/07/16.
 */
public class OSelectExecutionPlanner {

  private OSelectStatement oSelectStatement;
  private OProjection projection = null;
  private boolean     expand     = false;
  private boolean     distinct   = false;

  private boolean orderApplied          = false;
  private boolean projectionsCalculated = false;

  public OSelectExecutionPlanner(OSelectStatement oSelectStatement) {
    this.oSelectStatement = oSelectStatement;
  }

  public OExecutionPlan createExecutionPlan(OCommandContext ctx) {
    OSelectExecutionPlan result = new OSelectExecutionPlan(ctx);
    this.projection = oSelectStatement.getProjection();
    if (projection != null && this.projection.isExpand())

    {
      expand = true;
      this.projection = projection.getExpandContent();
    }

    OFromClause queryTarget = oSelectStatement.getTarget();
    if (queryTarget == null)

    {
      result.chain(new NoTargetProjectionEvaluator(oSelectStatement.getProjection(), ctx));
    } else

    {
      OFromItem target = queryTarget.getItem();
      if (target.getIdentifier() != null) {
        handleClassAsTarget(result, target.getIdentifier(), oSelectStatement, ctx);
      } else {
        throw new UnsupportedOperationException();
      }
    }

    handleLet(result, oSelectStatement.getLetClause(), ctx);
    handleProjectionsBeforeWhere(result, projection, oSelectStatement.getWhereClause(), ctx);
    handleWhere(result, oSelectStatement.getWhereClause(), ctx);
    handleExpand(result, ctx);

    if (oSelectStatement.getOrderBy() != null) {
      handleOrderBy(result, oSelectStatement.getOrderBy(), ctx);
    }

    if (oSelectStatement.getSkip() != null) {
      result.chain(new SkipExecutionStep(oSelectStatement.getSkip(), ctx));
    }

    if (oSelectStatement.getLimit() != null) {
      result.chain(new LimitExecutionStep(oSelectStatement.getLimit(), ctx));
    }

    return result;
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

  private void handleProjectionsBeforeWhere(OSelectExecutionPlan result, OProjection projection, OWhereClause whereClause, OCommandContext ctx) {
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

  private void handleClassAsTarget(OSelectExecutionPlan plan, OIdentifier identifier, OSelectStatement oSelectStatement,
      OCommandContext ctx) {
    //TODO optimize fetch from class, eg. when you can use and index or when you can do early sort.

    Boolean orderByRidAsc = null;//null: no order. true: asc, false:desc
    if (isOrderByRidAsc(oSelectStatement)) {
      orderByRidAsc = true;
    } else if (isOrderByRidDesc(oSelectStatement)) {
      orderByRidAsc = false;
    }
    FetchFromClassExecutionStep fetcher = new FetchFromClassExecutionStep(identifier.getStringValue(), oSelectStatement, ctx,
        orderByRidAsc);
    if (orderByRidAsc != null) {
      this.orderApplied = true;
    }
    plan.chain(fetcher);

  }

  private boolean isOrderByRidDesc(OSelectStatement stm) {
    if (!hasClassAsTarget(stm)) {
      return false;
    }
    OOrderBy orderBy = stm.getOrderBy();
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

  private boolean isOrderByRidAsc(OSelectStatement stm) {
    if (!hasClassAsTarget(stm)) {
      return false;
    }
    OOrderBy orderBy = stm.getOrderBy();
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

  private boolean hasClassAsTarget(OSelectStatement stm) {
    OFromClause target = stm.getTarget();
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
