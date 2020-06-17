package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OProjectionItem;
import java.util.Map;
import java.util.Optional;

public class GuaranteeEmptyCountStep extends AbstractExecutionStep {

  private final OProjectionItem item;
  private boolean executed = false;

  public GuaranteeEmptyCountStep(
      OProjectionItem oProjectionItem, OCommandContext ctx, boolean enableProfiling) {
    super(ctx, enableProfiling);
    this.item = oProjectionItem;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    if (!prev.isPresent()) {
      throw new IllegalStateException("filter step requires a previous step");
    }
    OResultSet upstream = prev.get().syncPull(ctx, nRecords);
    return new OResultSet() {
      @Override
      public boolean hasNext() {
        if (!executed) {
          return true;
        }

        return upstream.hasNext();
      }

      @Override
      public OResult next() {
        if (!hasNext()) {
          throw new IllegalStateException();
        }

        try {
          if (upstream.hasNext()) {
            return upstream.next();
          }
          OResultInternal result = new OResultInternal();
          result.setProperty(item.getProjectionAliasAsString(), 0L);
          return result;
        } finally {
          executed = true;
        }
      }

      @Override
      public void close() {
        prev.get().close();
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
