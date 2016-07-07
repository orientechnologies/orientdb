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

      lastStep = new NoTargetProjectionEvaluator(oSelectStatement.getProjection(), ctx);
      steps.add(lastStep);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override public void close() {
    lastStep.close();
  }

  @Override public OTodoResultSet fetchNext(int n) {
    return lastStep.syncPull(ctx, n);
  }

}
