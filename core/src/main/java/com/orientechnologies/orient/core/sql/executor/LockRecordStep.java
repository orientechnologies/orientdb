package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.storage.OStorage;
import java.util.Map;
import java.util.Optional;

public class LockRecordStep extends AbstractExecutionStep {
  private final OStorage.LOCKING_STRATEGY lockStrategy;

  public LockRecordStep(
      OStorage.LOCKING_STRATEGY lockStrategy, OCommandContext ctx, boolean enableProfiling) {
    super(ctx, enableProfiling);
    this.lockStrategy = lockStrategy;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    OResultSet upstream = getPrev().get().syncPull(ctx, nRecords);
    return new OResultSet() {
      @Override
      public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override
      public OResult next() {
        OResult result = upstream.next();
        result
            .getElement()
            .ifPresent(x -> ctx.getDatabase().getTransaction().lockRecord(x, lockStrategy));
        return result;
      }

      @Override
      public void close() {
        upstream.close();
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
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ LOCK RECORD");
    result.append("\n");
    result.append(spaces);
    result.append("  lock strategy: " + lockStrategy);

    return result.toString();
  }
}
