package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OFilterResultSet;
import com.orientechnologies.orient.core.sql.executor.resultset.OLimitedResultSet;
import java.util.List;

public class FilterNotMatchPatternStep extends AbstractExecutionStep {

  private List<AbstractExecutionStep> subSteps;

  private OResultSet prevResult = null;

  private long cost;

  public FilterNotMatchPatternStep(
      List<AbstractExecutionStep> steps, OCommandContext ctx, boolean enableProfiling) {
    super(ctx, enableProfiling);
    this.subSteps = steps;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    if (!prev.isPresent()) {
      throw new IllegalStateException("filter step requires a previous step");
    }
    OExecutionStepInternal prevStep = prev.get();

    return new OLimitedResultSet(
        new OFilterResultSet(
            () -> {
              if (prevResult == null) {
                prevResult = prevStep.syncPull(ctx, nRecords);
              } else if (!prevResult.hasNext()) {
                prevResult = prevStep.syncPull(ctx, nRecords);
              }
              return prevResult;
            },
            (result) -> {
              long begin = profilingEnabled ? System.nanoTime() : 0;
              try {
                if (!matchesPattern(result, ctx)) {
                  return result;
                }
              } finally {
                if (profilingEnabled) {
                  cost += (System.nanoTime() - begin);
                }
              }
              return null;
            }),
        nRecords);
  }

  private boolean matchesPattern(OResult nextItem, OCommandContext ctx) {
    OSelectExecutionPlan plan = createExecutionPlan(nextItem, ctx);
    try (OResultSet rs = plan.fetchNext(1)) {
      return rs.hasNext();
    }
  }

  private OSelectExecutionPlan createExecutionPlan(OResult nextItem, OCommandContext ctx) {
    OSelectExecutionPlan plan = new OSelectExecutionPlan(ctx);
    plan.chain(
        new AbstractExecutionStep(ctx, profilingEnabled) {
          private boolean executed = false;

          @Override
          public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
            OInternalResultSet result = new OInternalResultSet();
            if (!executed) {
              result.add(copy(nextItem));
              executed = true;
            }
            return result;
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
