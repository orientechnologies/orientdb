package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import java.util.Iterator;
import java.util.List;

/** Created by luigidellaquila on 19/09/16. */
public class ForEachStep extends AbstractExecutionStep {
  private final OIdentifier loopVariable;
  private final OExpression source;
  public List<OStatement> body;

  public ForEachStep(
      OIdentifier loopVariable, OExpression oExpression, List<OStatement> statements) {
    super();
    this.loopVariable = loopVariable;
    this.source = oExpression;
    this.body = statements;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    OExecutionStream prevStream = prev.get().start(ctx);
    prevStream.close(ctx);
    Iterator<Object> iterator = init(ctx);
    while (iterator.hasNext()) {
      ctx.setVariable(loopVariable.getStringValue(), iterator.next());
      OScriptExecutionPlan plan = initPlan(ctx);
      OExecutionStream result = plan.start(ctx);
      if (result.isTermination(ctx)) {
        return result;
      }
    }

    return new EmptyStep().start(ctx);
  }

  protected Iterator<Object> init(OCommandContext ctx) {
    Object val = source.execute(new OResultInternal(), ctx);
    return OMultiValue.getMultiValueIterator(val);
  }

  public OScriptExecutionPlan initPlan(OCommandContext ctx) {
    OBasicCommandContext subCtx1 = new OBasicCommandContext(ctx.getDatabase());
    subCtx1.setParent(ctx);
    OScriptExecutionPlan plan = new OScriptExecutionPlan();
    for (OStatement stm : body) {
      plan.chain(stm);
    }
    return plan;
  }
}
