package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.ArrayList;
import java.util.List;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OInsertExecutionPlan extends OSelectExecutionPlan {

  private List<OResult> result = new ArrayList<>();
  private int next = 0;

  public OInsertExecutionPlan(OCommandContext ctx) {
    super(ctx);
  }

  @Override
  public OExecutionStream start() {
    if (next >= result.size()) {
      return OExecutionStream.empty(); // empty
    }

    next = result.size();
    return OExecutionStream.resultIterator(result.iterator());
  }

  @Override
  public void reset(OCommandContext ctx) {
    result.clear();
    next = 0;
    super.reset(ctx);
    executeInternal();
  }

  public void executeInternal() throws OCommandExecutionException {
    OExecutionStream nextBlock = super.start();
    while (nextBlock.hasNext(ctx)) {
      result.add(nextBlock.next(ctx));
    }
  }

  @Override
  public OResult toResult() {
    OResultInternal res = (OResultInternal) super.toResult();
    res.setProperty("type", "InsertExecutionPlan");
    return res;
  }

  @Override
  public OInternalExecutionPlan copy(OCommandContext ctx) {
    OInsertExecutionPlan copy = new OInsertExecutionPlan(ctx);
    super.copyOn(copy, ctx);
    return copy;
  }

  @Override
  public boolean canBeCached() {
    return super.canBeCached();
  }
}
