package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.db.document.OQueryLifecycleListener;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.executor.OResultSetInternal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Created by luigidellaquila on 05/12/16. */
public class OLocalResultSetLifecycleDecorator implements OResultSetInternal {

  private OResultSetInternal entity;
  private List<OQueryLifecycleListener> lifecycleListeners = new ArrayList<>();
  private String queryId;
  private boolean includePlan = false;
  private boolean hasNextPage;
  private boolean closed = false;

  public OLocalResultSetLifecycleDecorator(OResultSetInternal rsCopy, String queryId) {
    this.entity = rsCopy;
    this.queryId = queryId;
  }

  public void addLifecycleListener(OQueryLifecycleListener db) {
    this.lifecycleListeners.add(db);
  }

  @Override
  public boolean hasNext() {
    if (closed) {
      return false;
    } else {
      boolean hasNext = entity.hasNext();
      if (!hasNext) {
        close();
      }
      return hasNext;
    }
  }

  @Override
  public OResult next() {
    if (!hasNext()) {
      throw new IllegalStateException();
    }
    OResult result = entity.next();
    return result;
  }

  @Override
  public void close() {
    if (!closed) {
      entity.close();
      this.lifecycleListeners.forEach(x -> x.queryClosed(this.getQueryId()));
      this.lifecycleListeners.clear();
      closed = true;
    }
  }

  @Override
  public Optional<OExecutionPlan> getExecutionPlan() {
    return entity.getExecutionPlan();
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return entity.getQueryStats();
  }

  public String getQueryId() {
    return queryId;
  }

  public boolean hasNextPage() {
    return hasNextPage;
  }

  public void setHasNextPage(boolean b) {
    this.hasNextPage = b;
  }

  public boolean isDetached() {
    return entity.isDetached();
  }

  public OResultSet getInternal() {
    return entity;
  }

  public void setIncludePlan(boolean includePlan) {
    this.includePlan = includePlan;
  }

  public boolean isIncludePlan() {
    return includePlan;
  }

  @Override
  public boolean isExplain() {
    return entity.isExplain();
  }
}
