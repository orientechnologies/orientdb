package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OProjectionItem;

public class GuaranteeEmptyCountStep extends AbstractExecutionStep {

  private final OProjectionItem item;

  public GuaranteeEmptyCountStep(OProjectionItem oProjectionItem) {
    super();
    this.item = oProjectionItem;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    if (!prev.isPresent()) {
      throw new IllegalStateException("filter step requires a previous step");
    }
    OExecutionStream upstream = prev.get().start(ctx);
    if (upstream.hasNext(ctx)) {
      return upstream.limit(1);
    } else {
      OResultInternal result = new OResultInternal();
      result.setProperty(item.getProjectionAliasAsString(), 0L);
      return OExecutionStream.singleton(result);
    }
  }

  @Override
  public OExecutionStepInternal copy(OCommandContext ctx) {
    return new GuaranteeEmptyCountStep(item.copy());
  }

  public boolean canBeCached() {
    return true;
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    StringBuilder result = new StringBuilder();
    result.append(OExecutionStepInternal.getIndent(ctx) + "+ GUARANTEE FOR ZERO COUNT ");
    return result.toString();
  }
}
