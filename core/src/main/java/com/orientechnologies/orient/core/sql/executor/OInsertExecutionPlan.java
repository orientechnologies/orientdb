package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OInsertExecutionPlan extends OSelectExecutionPlan {

  private List<OResult> result = new ArrayList<>();

  public OInsertExecutionPlan(OCommandContext ctx) {
    super(ctx);
  }

  @Override
  public OExecutionStream start() {
    return OExecutionStream.resultIterator(result.iterator());
  }

  @Override
  public void reset(OCommandContext ctx) {
    result.clear();
    super.reset(ctx);
    executeInternal();
  }

  public void executeInternal() throws OCommandExecutionException {
    OExecutionStream nextBlock = super.start();
    while (nextBlock.hasNext(ctx)) {
      result.add(nextBlock.next(ctx));
    }
    nextBlock.close(ctx);
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
