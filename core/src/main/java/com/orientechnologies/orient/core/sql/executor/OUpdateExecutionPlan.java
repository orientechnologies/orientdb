package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OUpdateExecutionPlan extends OSelectExecutionPlan {

  public OUpdateExecutionPlan() {
    super();
  }

  @Override
  public OExecutionStream start(OCommandContext ctx) {
    return OExecutionStream.collectAll(super.start(ctx), ctx);
  }

  @Override
  public OResult toResult(OToResultContext ctx) {
    OResultInternal res = (OResultInternal) super.toResult(ctx);
    res.setProperty("type", "UpdateExecutionPlan");
    return res;
  }

  @Override
  public boolean canBeCached() {
    for (OExecutionStepInternal step : steps) {
      if (!step.canBeCached()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public OInternalExecutionPlan copy(OCommandContext ctx) {
    OUpdateExecutionPlan copy = new OUpdateExecutionPlan();
    super.copyOn(copy, ctx);
    return copy;
  }
}
