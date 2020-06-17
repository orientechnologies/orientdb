package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    return new OResultSet() {
      public boolean finished = false;

      private OResult nextItem = null;
      private int fetched = 0;

      private void fetchNextItem() {
        nextItem = null;
        if (finished) {
          return;
        }
        if (prevResult == null) {
          prevResult = prevStep.syncPull(ctx, nRecords);
          if (!prevResult.hasNext()) {
            finished = true;
            return;
          }
        }
        while (!finished) {
          while (!prevResult.hasNext()) {
            prevResult = prevStep.syncPull(ctx, nRecords);
            if (!prevResult.hasNext()) {
              finished = true;
              return;
            }
          }
          nextItem = prevResult.next();
          long begin = profilingEnabled ? System.nanoTime() : 0;
          try {
            if (!matchesPattern(nextItem, ctx)) {
              break;
            }

            nextItem = null;
          } finally {
            if (profilingEnabled) {
              cost += (System.nanoTime() - begin);
            }
          }
        }
      }

      @Override
      public boolean hasNext() {

        if (fetched >= nRecords || finished) {
          return false;
        }
        if (nextItem == null) {
          fetchNextItem();
        }

        if (nextItem != null) {
          return true;
        }

        return false;
      }

      @Override
      public OResult next() {
        if (fetched >= nRecords || finished) {
          throw new IllegalStateException();
        }
        if (nextItem == null) {
          fetchNextItem();
        }
        if (nextItem == null) {
          throw new IllegalStateException();
        }
        OResult result = nextItem;
        nextItem = null;
        fetched++;
        return result;
      }

      @Override
      public void close() {
        FilterNotMatchPatternStep.this.close();
      }

      @Override
      public Optional<OExecutionPlan> getExecutionPlan() {
        return Optional.empty();
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };
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
