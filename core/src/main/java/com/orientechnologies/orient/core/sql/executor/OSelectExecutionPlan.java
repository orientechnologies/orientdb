package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by luigidellaquila on 06/07/16.
 */
public class OSelectExecutionPlan implements OExecutionPlan {

  protected List<OExecutionStep> steps = new ArrayList<>();
  OExecutionStep lastStep = null;
  private OCommandContext ctx;

  private OProjection projection = null;
  private boolean     expand     = false;
  private boolean     distinct   = false;

  private boolean orderApplied = false;
  private boolean projectionsCalculated = false;

  public OSelectExecutionPlan(OSelectStatement oSelectStatement, OCommandContext ctx) {
    this.ctx = ctx;

    this.projection = oSelectStatement.getProjection();
    if (this.projection.isExpand()) {
      expand = true;
      this.projection = projection.getExpandContent();
    }

    OFromClause queryTarget = oSelectStatement.getTarget();
    if (queryTarget == null) {
      chain(new NoTargetProjectionEvaluator(oSelectStatement.getProjection(), ctx));
    } else {
      OFromItem target = queryTarget.getItem();
      if (target.getIdentifier() != null) {
        handleClassAsTarget(target.getIdentifier(), oSelectStatement, ctx);
      } else {
        throw new UnsupportedOperationException();
      }
    }

    handleLet(oSelectStatement.getLetClause(), ctx);

    handleProjectionsBeforeWhere(projection, oSelectStatement.getWhereClause(), ctx);

    handleWhere(oSelectStatement.getWhereClause(), ctx);

    if (oSelectStatement.getOrderBy() != null) {
      handleOrderBy(oSelectStatement.getOrderBy(), ctx);
    }
    if (oSelectStatement.getSkip() != null) {
      chain(new SkipExecutionStep(oSelectStatement.getSkip(), ctx));
    }
    if (oSelectStatement.getLimit() != null) {
      chain(new LimitExecutionStep(oSelectStatement.getLimit(), ctx));
    }
  }

  private void handleLet(OLetClause letClause, OCommandContext ctx) {
    //TODO
  }

  private void handleWhere(OWhereClause whereClause, OCommandContext ctx) {
    //TODO
  }

  private void handleProjectionsBeforeWhere(OProjection projection, OWhereClause whereClause, OCommandContext ctx) {
    if(whereClause != null && projection != null){
      Set<String> aliases = projection.getAllAliases();
      if(whereClause.needsAliases(aliases)){
        //TODO
      }
    }
  }

  private void handleOrderBy(OOrderBy orderBy, OCommandContext ctx) {
    if (!orderApplied && orderBy != null && orderBy.getItems() != null && orderBy.getItems().size() > 0) {
      chain(new OrderByStep(orderBy, ctx));
    }
  }

  private void handleClassAsTarget(OIdentifier identifier, OSelectStatement oSelectStatement, OCommandContext ctx) {
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
    chain(fetcher);

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

  private void chain(OExecutionStep nextStep) {
    if (lastStep != null) {
      lastStep.setNext(nextStep);
      nextStep.setPrevious(lastStep);
    }
    lastStep = nextStep;
    steps.add(nextStep);
  }

  @Override public void close() {
    lastStep.close();
  }

  @Override public OTodoResultSet fetchNext(int n) {
    return lastStep.syncPull(ctx, n);
  }

  @Override public String prettyPrint(int indent) {
    StringBuilder result = new StringBuilder();
    for (OExecutionStep step : steps) {
      result.append(step.prettyPrint(0, indent));
      result.append("\n");
    }
    return result.toString();
  }

}
