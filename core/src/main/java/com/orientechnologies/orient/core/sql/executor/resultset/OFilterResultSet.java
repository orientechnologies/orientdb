package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Map;
import java.util.Optional;

public class OFilterResultSet implements OResultSet {

  public interface OResultSetProvider {
    OResultSet nextSet();
  }

  public interface OFilterResult {
    OResult filterMap(OResult result);
  }

  public OFilterResultSet(OResultSetProvider provider, OFilterResult filter) {
    super();
    this.provider = provider;
    this.filter = filter;
  }

  private OResultSetProvider provider;
  private OResultSet prevResult;
  private OFilterResult filter;
  public boolean finished = false;
  private OResult nextItem = null;

  private void fetchNextItem() {
    nextItem = null;
    if (finished) {
      return;
    }
    if (prevResult == null) {
      prevResult = provider.nextSet();
      if (!prevResult.hasNext()) {
        finished = true;
        return;
      }
    }
    while (!finished) {
      while (!prevResult.hasNext()) {
        prevResult = provider.nextSet();
        if (!prevResult.hasNext()) {
          finished = true;
          return;
        }
      }
      nextItem = prevResult.next();
      nextItem = filter.filterMap(nextItem);
      if (nextItem != null) {
        break;
      }
    }
  }

  @Override
  public boolean hasNext() {
    if (finished) {
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
    if (finished) {
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
}
