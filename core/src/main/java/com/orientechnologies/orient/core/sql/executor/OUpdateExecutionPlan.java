package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.ArrayList;
import java.util.List;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OUpdateExecutionPlan extends OSelectExecutionPlan {

  private List<OResult> result = new ArrayList<>();

  public OUpdateExecutionPlan() {
    super();
  }

  @Override
  public OExecutionStream start(OCommandContext ctx) {
    return OExecutionStream.resultIterator(result.iterator());
  }

  @Override
  public void reset(OCommandContext ctx) {
    result.clear();
    super.reset(ctx);
    executeInternal(ctx);
  }

  public void executeInternal(OCommandContext ctx) throws OCommandExecutionException {
    OExecutionStream nextBlock = super.start(ctx);
    while (nextBlock.hasNext(ctx)) {
      result.add(nextBlock.next(ctx));
    }
    nextBlock.close(ctx);
  }

  @Override
  public OResult toResult() {
    OResultInternal res = (OResultInternal) super.toResult();
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
