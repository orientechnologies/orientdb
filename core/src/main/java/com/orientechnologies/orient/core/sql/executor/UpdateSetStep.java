package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OUpdateItem;
import java.util.List;

/** Created by luigidellaquila on 09/08/16. */
public class UpdateSetStep extends AbstractExecutionStep {
  private final List<OUpdateItem> items;

  public UpdateSetStep(
      List<OUpdateItem> updateItems, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.items = updateItems;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    OExecutionStream upstream = getPrev().get().start(ctx);
    return upstream.map(this::mapResult);
  }

  private OResult mapResult(OResult result, OCommandContext ctx) {
    if (result instanceof OResultInternal) {
      for (OUpdateItem item : items) {
        OClass type = result.getElement().flatMap(x -> x.getSchemaType()).orElse(null);
        if (type == null) {
          Object clazz = result.getProperty("@view");
          if (clazz instanceof String) {
            type = ctx.getDatabase().getMetadata().getSchema().getView((String) clazz);
          }
        }

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
    result.append("+ UPDATE SET");
    for (int i = 0; i < items.size(); i++) {
      OUpdateItem item = items.get(i);
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
