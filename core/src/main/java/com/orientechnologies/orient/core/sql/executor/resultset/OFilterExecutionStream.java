package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OResult;

public class OFilterExecutionStream implements OExecutionStream {

  private OExecutionStream prevResult;
  private OFilterResult filter;
  private OResult nextItem = null;

  public OFilterExecutionStream(OExecutionStream resultSet, OFilterResult filter) {
    super();
    this.prevResult = resultSet;
    this.filter = filter;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    if (nextItem == null) {
      fetchNextItem(ctx);
    }

    if (nextItem != null) {
      return true;
    }

    return false;
  }

  @Override
  public OResult next(OCommandContext ctx) {
    if (nextItem == null) {
      fetchNextItem(ctx);
    }
    if (nextItem == null) {
      throw new IllegalStateException();
    }
    OResult result = nextItem;
    nextItem = null;
    return result;
  }

  @Override
  public void close(OCommandContext ctx) {
    this.prevResult.close(ctx);
  }

  private void fetchNextItem(OCommandContext ctx) {
    while (prevResult.hasNext(ctx)) {
      nextItem = prevResult.next(ctx);
      nextItem = filter.filterMap(nextItem, ctx);
      if (nextItem != null) {
        break;
      }
    }
  }
}
