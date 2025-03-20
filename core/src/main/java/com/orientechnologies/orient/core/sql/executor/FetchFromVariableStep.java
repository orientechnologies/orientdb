package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OFromItem;

/** Created by luigidellaquila on 22/07/16. */
public class FetchFromVariableStep extends AbstractExecutionStep {

  private OFromItem variableName;

  public FetchFromVariableStep(OFromItem variableName) {
    super();
    this.variableName = variableName;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.start(ctx).close(ctx));
    String name = variableName.getIdentifier().getStringValue();
    Object value = ctx.getVariable(name);
    if (variableName.getModifier() != null) {
      value = variableName.getModifier().execute((OResult) null, value, ctx);
    }
    final Object src = value;
    OExecutionStream source;
    if (src instanceof OExecutionStream) {
      source = (OExecutionStream) src;
    } else if (src instanceof OResultSet) {
      source =
          OExecutionStream.resultIterator(((OResultSet) src).stream().iterator())
              .onClose((context) -> ((OResultSet) src).close());
    } else if (src instanceof ORID) {
      source =
          OExecutionStream.singleton(
              new OResultInternal((OIdentifiable) ctx.getDatabase().load((ORID) src)));
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
  public String prettyPrint(OPrintContext ctx) {
    return OExecutionStepInternal.getIndent(ctx)
        + "+ FETCH FROM VARIABLE\n"
        + OExecutionStepInternal.getIndent(ctx)
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
    } catch (Exception e) {
      throw OException.wrapException(new OCommandExecutionException(""), e);
    }
  }
}
