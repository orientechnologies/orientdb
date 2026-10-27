package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.util.OResettable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Created by luigidellaquila on 07/07/16. */
public class OResultSetReady implements OResultSetInternal, OResettable {
  private List<OResult> content = new ArrayList<>();
  private int next = 0;
  protected OExecutionPlan plan;

  public OResultSetReady() {}

  public static OResultSetReady fromResultSet(OResultSet from) {
    OResultSetReady result = new OResultSetReady();
    from.forEachRemaining(result::add);
    from.getExecutionPlan().ifPresent(result::setPlan);
    from.close();
    return result;
  }

  public static OResultSet loadIfNeeded(OResultSet from) {
    if (from instanceof OResultSetReady) {
      return from;
    } else {
      return fromResultSet(from);
    }
  }

  @Override
  public boolean hasNext() {
    return content.size() > next;
  }

  @Override
  public OResult next() {
    return content.get(next++);
  }

  @Override
  public void close() {
    this.content.clear();
  }

  @Override
  public Optional<OExecutionPlan> getExecutionPlan() {
    return Optional.ofNullable(plan);
  }

  public void setPlan(OExecutionPlan plan) {
    this.plan = plan;
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return new HashMap<>();
  }

  public void add(OResult nextResult) {
    content.add(nextResult);
  }

  public void reset() {
    this.next = 0;
  }

  public int size() {
    return content.size();
  }

  public OResultSetReady copy() {
    OResultSetReady result = new OResultSetReady();
    result.content = this.content;
    result.plan = this.plan;
    return result;
  }

  @Override
  public boolean isDetached() {
    return true;
  }

  @Override
  public boolean isExplain() {
    return false;
  }
}
