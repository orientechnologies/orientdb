package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.resultset.OIteratorResultSet;
import com.orientechnologies.orient.core.sql.executor.resultset.OSubResultsResultSet;
import java.util.Collection;

/**
 * unwinds a result-set.
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public abstract class AbstractUnrollStep extends AbstractExecutionStep {

  public AbstractUnrollStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx) throws OTimeoutException {
    if (prev == null || !prev.isPresent()) {
      throw new OCommandExecutionException("Cannot expand without a target");
    }
    OResultSet resultSet = getPrev().get().syncPull(ctx);
    OResultSet result =
        new OSubResultsResultSet(
            resultSet.stream().map((res) -> fetchNextResults(ctx, res)).iterator());
    return result;
  }

  private OResultSet fetchNextResults(OCommandContext ctx, OResult res) {
    return (OResultSet) new OIteratorResultSet(unroll(res, ctx).iterator());
  }

  protected abstract Collection<OResult> unroll(final OResult doc, final OCommandContext iContext);
}
