package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OTimeout;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/** Created by luigidellaquila on 08/08/16. */
public class AccumulatingTimeoutStep extends AbstractExecutionStep {
  private final OTimeout timeout;
  private final long timeoutMillis;

  private AtomicLong totalTime = new AtomicLong(0);

  public AccumulatingTimeoutStep(OTimeout timeout, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.timeout = timeout;
    this.timeoutMillis = this.timeout.getVal().longValue();
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {

    final OResultSet internal = getPrev().get().syncPull(ctx, nRecords);
    return new OResultSet() {

      @Override
      public boolean hasNext() {
        if (timedOut) {
          return false;
        }
        long begin = System.nanoTime();

        try {
          return internal.hasNext();
        } finally {
          totalTime.addAndGet(System.nanoTime() - begin);
        }
      }

      @Override
      public OResult next() {
        if (totalTime.get() / 1_000_000 > timeoutMillis) {
          fail();
          if (timedOut) {
            return new OResultInternal();
          }
        }
        long begin = System.nanoTime();
        try {
          return internal.next();
        } finally {
          totalTime.addAndGet(System.nanoTime() - begin);
        }
      }

      @Override
      public void close() {
        internal.close();
      }

      @Override
      public Optional<OExecutionPlan> getExecutionPlan() {
        return internal.getExecutionPlan();
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return internal.getQueryStats();
      }
    };
  }

  private void fail() {
    this.timedOut = true;
    if (OTimeout.RETURN.equals(this.timeout.getFailureStrategy())) {
      // do nothing
    } else {
      sendTimeout();
      throw new OTimeoutException("Timeout expired");
    }
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new AccumulatingTimeoutStep(timeout.copy(), ctx, profilingEnabled);
  }

  @Override
  public void reset() {
    this.totalTime = new AtomicLong(0);
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    return OExecutionStepInternal.getIndent(depth, indent)
        + "+ TIMEOUT ("
        + timeout.getVal().toString()
        + "ms)";
  }
}
