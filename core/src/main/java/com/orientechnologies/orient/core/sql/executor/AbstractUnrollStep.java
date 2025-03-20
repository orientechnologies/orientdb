package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import java.util.Collection;

/**
 * unwinds a result-set.
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public abstract class AbstractUnrollStep extends AbstractExecutionStep {

  public AbstractUnrollStep() {
    super();
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    if (prev == null || !prev.isPresent()) {
      throw new OCommandExecutionException("Cannot expand without a target");
    }
    OExecutionStream resultSet = getPrev().get().start(ctx);
    return resultSet.flatMap(this::fetchNextResults);
  }

  private OExecutionStream fetchNextResults(OResult res, OCommandContext ctx) {
    return OExecutionStream.resultCollection(unroll(res, ctx));
  }

  protected abstract Collection<OResult> unroll(final OResult doc, final OCommandContext iContext);
}
