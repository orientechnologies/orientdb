package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * unwinds a result-set.
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public abstract class AbstractUnrollStep extends AbstractExecutionStep {

  private Iterator<OResult> nextSubsequence = null;
  private OResult nextElement = null;

  public AbstractUnrollStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public void reset() {
    this.nextSubsequence = null;
    this.nextElement = null;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx) throws OTimeoutException {
    if (prev == null || !prev.isPresent()) {
      throw new OCommandExecutionException("Cannot expand without a target");
    }
    OResultSet resultSet = getPrev().get().syncPull(ctx);
    return new OResultSet() {

      @Override
      public boolean hasNext() {
        if (nextElement == null) {
          fetchNext(ctx, resultSet);
        }
        if (nextElement == null) {
          return false;
        }
        return true;
      }

      @Override
      public OResult next() {
        if (nextElement == null) {
          fetchNext(ctx, resultSet);
        }
        if (nextElement == null) {
          throw new IllegalStateException();
        }

        OResult result = nextElement;
        nextElement = null;
        fetchNext(ctx, resultSet);
        return result;
      }

      @Override
      public void close() {}

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

  private void fetchNext(OCommandContext ctx, OResultSet resultSet) {
    do {
      if (nextSubsequence != null && nextSubsequence.hasNext()) {
        nextElement = nextSubsequence.next();
        break;
      }

      if (nextSubsequence == null || !nextSubsequence.hasNext()) {
        if (!resultSet.hasNext()) {
          return;
        }
      }

      OResult nextAggregateItem = resultSet.next();
      nextSubsequence = unroll(nextAggregateItem, ctx).iterator();

    } while (true);
  }

  protected abstract Collection<OResult> unroll(final OResult doc, final OCommandContext iContext);
}
