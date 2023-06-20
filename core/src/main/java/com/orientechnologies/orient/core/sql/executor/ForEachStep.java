package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OForEachBlock;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import com.orientechnologies.orient.core.sql.parser.OIfStatement;
import com.orientechnologies.orient.core.sql.parser.OReturnStatement;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import java.util.Iterator;
import java.util.List;

/** Created by luigidellaquila on 19/09/16. */
public class ForEachStep extends AbstractExecutionStep {
  private final OIdentifier loopVariable;
  private final OExpression source;
  public List<OStatement> body;

  public ForEachStep(
      OIdentifier loopVariable,
      OExpression oExpression,
      List<OStatement> statements,
      OCommandContext ctx,
      boolean enableProfiling) {
    super(ctx, enableProfiling);
    this.loopVariable = loopVariable;
    this.source = oExpression;
    this.body = statements;
  }

  @Override
  public OExecutionStream syncPull(OCommandContext ctx) throws OTimeoutException {
    prev.get().syncPull(ctx);
    Iterator<Object> iterator = init(ctx);
    while (iterator.hasNext()) {
      ctx.setVariable(loopVariable.getStringValue(), iterator.next());
      OScriptExecutionPlan plan = initPlan(ctx);
      OExecutionStepInternal result = plan.executeFull();
      if (result != null) {
        return result.syncPull(ctx);
      }
    }

    return new EmptyStep(ctx, false).syncPull(ctx);
  }

  protected Iterator<Object> init(OCommandContext ctx) {
    Object val = source.execute(new OResultInternal(), ctx);
    return OMultiValue.getMultiValueIterator(val);
  }

  public OScriptExecutionPlan initPlan(OCommandContext ctx) {
    OBasicCommandContext subCtx1 = new OBasicCommandContext();
    subCtx1.setParent(ctx);
    OScriptExecutionPlan plan = new OScriptExecutionPlan(subCtx1);
    for (OStatement stm : body) {
      plan.chain(stm.createExecutionPlan(subCtx1, profilingEnabled), profilingEnabled);
    }
    return plan;
  }

  public boolean containsReturn() {
    for (OStatement stm : this.body) {
      if (stm instanceof OReturnStatement) {
        return true;
      }
      if (stm instanceof OForEachBlock && ((OForEachBlock) stm).containsReturn()) {
        return true;
      }
      if (stm instanceof OIfStatement && ((OIfStatement) stm).containsReturn()) {
        return true;
      }
    }
    return false;
  }
}
