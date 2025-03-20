package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import java.util.List;

public class FilterNotMatchPatternStep extends AbstractExecutionStep {

  private List<AbstractExecutionStep> subSteps;

  public FilterNotMatchPatternStep(List<AbstractExecutionStep> steps) {
    super();
    this.subSteps = steps;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    if (!prev.isPresent()) {
      throw new IllegalStateException("filter step requires a previous step");
    }
    OExecutionStream resultSet = prev.get().start(ctx);
    return resultSet.filter(this::filterMap);
  }

  private OResult filterMap(OResult result, OCommandContext ctx) {
    if (!matchesPattern(result, ctx)) {
      return result;
    }
    return null;
  }

  private boolean matchesPattern(OResult nextItem, OCommandContext ctx) {
    OSelectExecutionPlan plan = createExecutionPlan(nextItem, ctx);
    OExecutionStream rs = plan.start(ctx);
    try {
      return rs.hasNext(ctx);
    } finally {
      rs.close(ctx);
    }
  }

  private OSelectExecutionPlan createExecutionPlan(OResult nextItem, OCommandContext ctx) {
    OSelectExecutionPlan plan = new OSelectExecutionPlan();
    plan.chain(
        new AbstractExecutionStep() {

          @Override
          public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
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
  public List<OExecutionStepInternal> getSubSteps() {
    return (List) subSteps;
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    String spaces = OExecutionStepInternal.getIndent(ctx);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ NOT (\n");
    this.subSteps.forEach(
        x -> {
          ctx.incDepth();
          result.append(x.prettyPrint(ctx)).append("\n");
          ctx.decDepth();
        });
    result.append(spaces);
    result.append("  )");
    return result.toString();
  }
}
