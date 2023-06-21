package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.executor.resultset.OProduceExecutionStream;

/** Created by luigidellaquila on 08/07/16. */
public class EmptyDataGeneratorStep extends AbstractExecutionStep {

  private long cost = 0;

  private int size;

  public EmptyDataGeneratorStep(int size, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.size = size;
  }

  @Override
  public OExecutionStream syncPull(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx));
    return new OProduceExecutionStream(this::create).limit(size);
  }

  private OResult create(OCommandContext ctx) {
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      OResultInternal result = new OResultInternal();
      ctx.setVariable("$current", result);
      return result;
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ GENERATE " + size + " EMPTY " + (size == 1 ? "RECORD" : "RECORDS");
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

  @Override
  public long getCost() {
    return cost;
  }
}
