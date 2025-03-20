package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OInsertExecutionPlan extends OSelectExecutionPlan {

  public OInsertExecutionPlan() {
    super();
  }

  @Override
  public OExecutionStream start(OCommandContext ctx) {
    return OExecutionStream.collectAll(super.start(ctx), ctx);
  }

  @Override
  public OResult toResult(OToResultContext ctx) {
    OResultInternal res = (OResultInternal) super.toResult(ctx);
    res.setProperty("type", "InsertExecutionPlan");
    return res;
  }

  @Override
  public OInternalExecutionPlan copy(OCommandContext ctx) {
    OInsertExecutionPlan copy = new OInsertExecutionPlan();
    super.copyOn(copy, ctx);
    return copy;
  }

  @Override
  public boolean canBeCached() {
    return super.canBeCached();
  }
}
