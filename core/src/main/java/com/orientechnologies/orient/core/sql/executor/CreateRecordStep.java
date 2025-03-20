package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import java.util.Optional;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class CreateRecordStep extends AbstractExecutionStep {

  private int total = 0;
  private Optional<String> cl;

  public CreateRecordStep(int total, Optional<String> cl) {
    super();
    this.total = total;
    this.cl = cl;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.start(ctx).close(ctx));
    return OExecutionStream.produce(this::produce).limit(total);
  }

  private OResult produce(OCommandContext ctx) {
    if (cl.isPresent()) {
      return new OUpdatableResult((ODocument) ctx.getDatabase().newInstance(cl.get()));
    }
    return new OUpdatableResult((ODocument) ctx.getDatabase().newInstance());
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    String spaces = OExecutionStepInternal.getIndent(ctx);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ CREATE EMPTY RECORDS");
    if (ctx.isProfilingEnabled()) {
      result.append(" (" + ctx.getCostFormatted(this) + ")");
    }
    result.append("\n");
    result.append(spaces);
    if (total == 1) {
      result.append("  1 record");
    } else {
      result.append("  " + total + " record");
    }
    return result.toString();
  }
}
