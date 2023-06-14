package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OIteratorResultSet;
import com.orientechnologies.orient.core.sql.executor.resultset.OLimitedResultSet;
import com.orientechnologies.orient.core.sql.parser.OProjectionItem;
import java.util.Collections;

public class GuaranteeEmptyCountStep extends AbstractExecutionStep {

  private final OProjectionItem item;

  public GuaranteeEmptyCountStep(
      OProjectionItem oProjectionItem, OCommandContext ctx, boolean enableProfiling) {
    super(ctx, enableProfiling);
    this.item = oProjectionItem;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx) throws OTimeoutException {
    if (!prev.isPresent()) {
      throw new IllegalStateException("filter step requires a previous step");
    }
    OResultSet upstream = prev.get().syncPull(ctx);
    if (upstream.hasNext()) {
      return new OLimitedResultSet(upstream, 1);
    } else {
      OResultInternal result = new OResultInternal();
      result.setProperty(item.getProjectionAliasAsString(), 0L);
      return new OIteratorResultSet(Collections.singleton(result).iterator());
    }
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new GuaranteeEmptyCountStep(item.copy(), ctx, profilingEnabled);
  }

  public boolean canBeCached() {
    return true;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    StringBuilder result = new StringBuilder();
    result.append(OExecutionStepInternal.getIndent(depth, indent) + "+ GUARANTEE FOR ZERO COUNT ");
    return result.toString();
  }
}
