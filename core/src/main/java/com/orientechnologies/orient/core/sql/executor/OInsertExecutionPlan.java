package com.orientechnologies.orient.core.sql.executor;

/** Created by luigidellaquila on 08/08/16. */
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
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
  public OResultSet fetchNext() {
    if (next >= result.size()) {
      return new OInternalResultSet(); // empty
    }

    OIteratorResultSet nextBlock = new OIteratorResultSet(result.iterator());
    next = result.size();
    return nextBlock;
  }

  @Override
  public void reset(OCommandContext ctx) {
    result.clear();
    next = 0;
    super.reset(ctx);
    executeInternal();
  }

  public void executeInternal() throws OCommandExecutionException {
    OResultSet nextBlock = super.fetchNext();
    while (nextBlock.hasNext()) {
      result.add(nextBlock.next());
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
