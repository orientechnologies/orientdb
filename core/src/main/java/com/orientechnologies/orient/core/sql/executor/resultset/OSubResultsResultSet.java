package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.Iterator;

public final class OSubResultsResultSet implements OExecutionStream {
  private final Iterator<OExecutionStream> subSteps;
  private OExecutionStream currentResultSet;

  public OSubResultsResultSet(Iterator<OExecutionStream> subSteps) {
    this.subSteps = subSteps;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    while (currentResultSet == null || !currentResultSet.hasNext(ctx)) {
      if (currentResultSet != null) {
        currentResultSet.close(ctx);
      }
      if (!subSteps.hasNext()) {
        return false;
      }
      currentResultSet = subSteps.next();
    }
    return true;
  }

  @Override
  public OResult next(OCommandContext ctx) {
    if (!hasNext(ctx)) {
      throw new IllegalStateException();
    }
    return currentResultSet.next(ctx);
  }

  @Override
  public void close(OCommandContext ctx) {
    if (currentResultSet != null) {
      currentResultSet.close(ctx);
    }
    while (subSteps.hasNext()) {
      subSteps.next().close(ctx);
    }
  }
}
