package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.OElement;
import java.util.Iterator;

/** Created by luigidellaquila on 22/07/16. */
public class FetchFromVariableStep extends AbstractExecutionStep {

  private String variableName;
  private OResultSet source;
  private boolean inited = false;

  public FetchFromVariableStep(String variableName, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.variableName = variableName;
    reset();
  }

  public void reset() {}

  @Override
  public OResultSet syncPull(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx));
    init();
    return source;
  }

  private void init() {
    if (inited) {
      return;
    }
    inited = true;
    Object src = ctx.getVariable(variableName);
    if (src instanceof OInternalResultSet) {
      source = ((OInternalResultSet) src).copy();
    } else if (src instanceof OResultSet) {
      source = (OResultSet) src;
      source.reset();
    } else if (src instanceof OElement) {
      source = new OInternalResultSet();
      ((OInternalResultSet) source).add(new OResultInternal((OElement) src));
    } else if (src instanceof OResult) {
      source = new OInternalResultSet();
      ((OInternalResultSet) source).add((OResult) src);
    } else if (src instanceof Iterable) {
      source = new OInternalResultSet();

      Iterator iter = ((Iterable) src).iterator();
      while (iter.hasNext()) {
        Object next = iter.next();
        if (next instanceof OElement) {
          ((OInternalResultSet) source).add(new OResultInternal((OElement) next));
        } else if (next instanceof OResult) {
          ((OInternalResultSet) source).add((OResult) next);
        } else {
          OResultInternal item = new OResultInternal();
          item.setProperty("value", next);
          ((OInternalResultSet) source).add(item);
        }
      }
    } else {
      throw new OCommandExecutionException("Cannot use variable as query target: " + variableName);
    }
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
    result.setProperty("variableName", variableName);
    return result;
  }

  @Override
  public void deserialize(OResult fromResult) {
    try {
      OExecutionStepInternal.basicDeserialize(fromResult, this);
      if (fromResult.getProperty("variableName") != null) {
        this.variableName = fromResult.getProperty(variableName);
      }
      reset();
    } catch (Exception e) {
      throw OException.wrapException(new OCommandExecutionException(""), e);
    }
  }
}
