package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OFromClause;
import com.orientechnologies.orient.core.sql.parser.OFromItem;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import com.orientechnologies.orient.core.sql.parser.OSelectStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luigidellaquila on 06/07/16.
 */
public class OSelectExecutionPlan implements OExecutionPlan {

  protected List<OExecutionStep> steps = new ArrayList<>();
  OExecutionStep lastStep = null;
  private OCommandContext ctx;

  public OSelectExecutionPlan(OSelectStatement oSelectStatement, OCommandContext ctx) {
    this.ctx = ctx;
    OFromClause queryTarget = oSelectStatement.getTarget();
    if (queryTarget == null) {
      chain(new NoTargetProjectionEvaluator(oSelectStatement.getProjection(), ctx));
    } else {
      OFromItem target = queryTarget.getItem();
      if(target.getIdentifier()!=null){
        classAsTarget(target.getIdentifier(), oSelectStatement, ctx);
      }else {
        throw new UnsupportedOperationException();
      }
    }

    if (oSelectStatement.getSkip() != null) {
      chain(new SkipExecutionStep(oSelectStatement.getSkip(), ctx));
    }
    if (oSelectStatement.getLimit() != null) {
      chain(new LimitExecutionStep(oSelectStatement.getLimit(), ctx));
    }
  }

  private void classAsTarget(OIdentifier identifier, OSelectStatement oSelectStatement, OCommandContext ctx) {
    //TODO optimize fetch from class, eg. when you can use and index or when you can do early sort.
    chain(new FetchFromClassExecutionStep(identifier.getStringValue(), oSelectStatement, ctx));

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
