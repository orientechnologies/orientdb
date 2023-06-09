package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Map;
import java.util.Optional;

public class OFilterResultSet implements OResultSet {

  public interface OFilterResult {
    OResult filterMap(OResult result);
  }

  public OFilterResultSet(OResultSet resultSet, OFilterResult filter) {
    super();
    this.prevResult = resultSet;
    this.filter = filter;
  }

  private OResultSet prevResult;
  private OFilterResult filter;
  private OResult nextItem = null;

  private void fetchNextItem() {
    while (prevResult.hasNext()) {
      nextItem = prevResult.next();
      nextItem = filter.filterMap(nextItem);
      if (nextItem != null) {
        break;
      }
    }
  }

  @Override
  public boolean hasNext() {
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
