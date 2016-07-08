package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OFromClause;
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
      throw new UnsupportedOperationException();
    }
    if (oSelectStatement.getSkip() != null) {
      chain(new SkipExecutionStep(oSelectStatement.getSkip(), ctx));
    }
    if (oSelectStatement.getLimit() != null) {
      chain(new LimitExecutionStep(oSelectStatement.getLimit(), ctx));
    }
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

}
