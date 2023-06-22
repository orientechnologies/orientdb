package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.List;

public class FilterNotMatchPatternStep extends AbstractExecutionStep {

  private List<AbstractExecutionStep> subSteps;

  public FilterNotMatchPatternStep(
      List<AbstractExecutionStep> steps, OCommandContext ctx, boolean enableProfiling) {
    super(ctx, enableProfiling);
    this.subSteps = steps;
  }

  @Override
  public OExecutionStream syncPull(OCommandContext ctx) throws OTimeoutException {
    if (!prev.isPresent()) {
      throw new IllegalStateException("filter step requires a previous step");
    }
    OExecutionStream resultSet = prev.get().syncPull(ctx);
    return attachProfile(resultSet.filter(this::filterMap));
  }

  private OResult filterMap(OResult result, OCommandContext ctx) {
    if (!matchesPattern(result, ctx)) {
      return result;
    }
    return null;
  }

  private boolean matchesPattern(OResult nextItem, OCommandContext ctx) {
    OSelectExecutionPlan plan = createExecutionPlan(nextItem, ctx);
    OExecutionStream rs = plan.start();
    try {
      return rs.hasNext(ctx);
    } finally {
      rs.close(ctx);
    }
  }

  private OSelectExecutionPlan createExecutionPlan(OResult nextItem, OCommandContext ctx) {
    OSelectExecutionPlan plan = new OSelectExecutionPlan(ctx);
    plan.chain(
        new AbstractExecutionStep(ctx, profilingEnabled) {

          @Override
          public OExecutionStream syncPull(OCommandContext ctx) throws OTimeoutException {
            return OExecutionStream.singleton(copy(nextItem));
          }

          private OResult copy(OResult nextItem) {
            OResultInternal result = new OResultInternal();
            for (String prop : nextItem.getPropertyNames()) {
              result.setProperty(prop, nextItem.getProperty(prop));
            }
            for (String md : nextItem.getMetadataKeys()) {
              result.setMetadata(md, nextItem.getMetadata(md));
            }
            return result;
          }
        });
    subSteps.stream().forEach(step -> plan.chain(step));
    return plan;
  }

  @Override
  public List<OExecutionStep> getSubSteps() {
    return (List) subSteps;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ NOT (\n");
    this.subSteps.forEach(x -> result.append(x.prettyPrint(depth + 1, indent)).append("\n"));
    result.append(spaces);
    result.append("  )");
    return result.toString();
  }

  @Override
  public void close() {
    super.close();
  }
}
