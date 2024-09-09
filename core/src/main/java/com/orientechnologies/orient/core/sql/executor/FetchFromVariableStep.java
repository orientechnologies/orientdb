package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OFromItem;

/** Created by luigidellaquila on 22/07/16. */
public class FetchFromVariableStep extends AbstractExecutionStep {

  private OFromItem variableName;

  public FetchFromVariableStep(
      OFromItem variableName, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.variableName = variableName;
    reset();
  }

  public void reset() {}

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.start(ctx).close(ctx));
    Object src = ctx.getVariable(variableName.toString());
    OExecutionStream source;
    if (src instanceof OExecutionStream) {
      source = (OExecutionStream) src;
    } else if (src instanceof OResultSet) {
      source =
          OExecutionStream.resultIterator(((OResultSet) src).stream().iterator())
              .onClose((context) -> ((OResultSet) src).close());
    } else if (src instanceof ORID) {
      source = OExecutionStream.singleton(new OResultInternal(ctx.getDatabase().load((ORID) src)));
    } else if (src instanceof OElement) {
      source = OExecutionStream.singleton(new OResultInternal((OElement) src));
    } else if (src instanceof OResult) {
      source = OExecutionStream.singleton((OResult) src);
    } else if (src instanceof Iterable) {
      source = OExecutionStream.iterator(((Iterable) src).iterator());
    } else {
      throw new OCommandExecutionException("Cannot use variable as query target: " + variableName);
    }
    return source;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    return OExecutionStepInternal.getIndent(depth, indent)
        + "+ FETCH FROM VARIABLE\n"
        + OExecutionStepInternal.getIndent(depth, indent)
        + "  "
        + variableName;
  }

  @Override
  public OResult serialize() {
    OResultInternal result = OExecutionStepInternal.basicSerialize(this);
    result.setProperty("variableName", this.variableName.serialize());
    return result;
  }

  @Override
  public void deserialize(OResult fromResult) {
    try {
      OExecutionStepInternal.basicDeserialize(fromResult, this);
      if (fromResult.getProperty("variableName") != null) {
        this.variableName = new OFromItem(-1);
        this.variableName.deserialize(fromResult.getProperty("variableName"));
      }
      reset();
    } catch (Exception e) {
      throw OException.wrapException(new OCommandExecutionException(""), e);
    }
  }
}
