package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.db.document.OQueryLifecycleListener;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OInternalResultSet;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OTodoResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by luigidellaquila on 05/12/16.
 */
public class OLocalResultSetLifecycleDecorator implements OTodoResultSet {

  private static final AtomicLong counter = new AtomicLong(0);

  private OTodoResultSet entity;
  private List<OQueryLifecycleListener> lifecycleListeners = new ArrayList<>();
  private String queryId;

  private boolean hasNextPage;

  public OLocalResultSetLifecycleDecorator(OTodoResultSet entity) {
    this.entity = entity;
    queryId = "" + System.currentTimeMillis() + "_" + counter.incrementAndGet();
  }

  public OLocalResultSetLifecycleDecorator(OInternalResultSet rsCopy, String queryId) {
    this.entity = rsCopy;
    this.queryId = queryId;
  }

  public void addLifecycleListener(OQueryLifecycleListener db) {
    this.lifecycleListeners.add(db);
  }

  @Override public boolean hasNext() {
    return entity.hasNext();
  }

  @Override public OResult next() {
    return entity.next();
  }

  @Override public void close() {
    entity.close();
    this.lifecycleListeners.forEach(x -> x.queryClosed(this));
    this.lifecycleListeners.clear();
  }

  @Override public Optional<OExecutionPlan> getExecutionPlan() {
    return entity.getExecutionPlan();
  }

  @Override public Map<String, Long> getQueryStats() {
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
}
