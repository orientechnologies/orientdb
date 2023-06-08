package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OResultSetMapper;
import com.orientechnologies.orient.core.sql.parser.OUpdateRemoveItem;
import java.util.List;

/** Created by luigidellaquila on 09/08/16. */
public class UpdateRemoveStep extends AbstractExecutionStep {
  private final List<OUpdateRemoveItem> items;

  public UpdateRemoveStep(
      List<OUpdateRemoveItem> items, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.items = items;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx) throws OTimeoutException {
    OResultSet upstream = getPrev().get().syncPull(ctx);
    return new OResultSetMapper(upstream, (result) -> extracted(ctx, result));
  }

  private OResult extracted(OCommandContext ctx, OResult result) {
    if (result instanceof OResultInternal) {
      for (OUpdateRemoveItem item : items) {
        item.applyUpdate((OResultInternal) result, ctx);
      }
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ UPDATE REMOVE");
    for (int i = 0; i < items.size(); i++) {
      OUpdateRemoveItem item = items.get(i);
      if (i < items.size()) {
        result.append("\n");
      }
      result.append(spaces);
      result.append("  ");
      result.append(item.toString());
    }
    return result.toString();
  }
}
