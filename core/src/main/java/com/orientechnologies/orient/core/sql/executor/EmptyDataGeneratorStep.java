package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;

/** Created by luigidellaquila on 08/07/16. */
public class EmptyDataGeneratorStep extends AbstractExecutionStep {

  private int size;

  public EmptyDataGeneratorStep(int size) {
    super();
    this.size = size;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.start(ctx).close(ctx));
    return OExecutionStream.produce(this::create).limit(size);
  }

  private OResult create(OCommandContext ctx) {
    OResultInternal result = new OResultInternal();
    ctx.setCurrent(result);
    return result;
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    String spaces = OExecutionStepInternal.getIndent(ctx);
    String result = spaces + "+ GENERATE " + size + " EMPTY " + (size == 1 ? "RECORD" : "RECORDS");
    if (ctx.isProfilingEnabled()) {
      result += " (" + ctx.getCostFormatted(this) + ")";
    }
    return result;
  }
}
