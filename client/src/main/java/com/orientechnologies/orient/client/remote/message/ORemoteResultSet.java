package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentRemote;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OTodoResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 05/12/16.
 */
public class ORemoteResultSet implements OTodoResultSet {

  private final String                  queryId;
  private final ODatabaseDocumentRemote db;

  private List<OResult> currentPage = new ArrayList<>();
  private OExecutionPlan      executionPlan;
  private Map<String, Object> queryStats;

  public ORemoteResultSet(ODatabaseDocumentRemote db, String queryId) {
    this.db = db;
    this.queryId = queryId;
  }

  @Override public boolean hasNext() {
    if (!currentPage.isEmpty()) {
      return true;
    }
    fetchNextPage();
    return !currentPage.isEmpty();
  }

  private void fetchNextPage() {
    db.fetchNextPage(this);
  }

  @Override public OResult next() {
    if (currentPage.isEmpty()) {
      fetchNextPage();
    }
    if (currentPage.isEmpty()) {
      throw new IllegalStateException();
    }
    return currentPage.remove(0);
  }

  @Override public void close() {
    db.closeQuery(queryId);
  }

  @Override public Optional<OExecutionPlan> getExecutionPlan() {
    return Optional.ofNullable(executionPlan);
  }

  @Override public Map<String, Object> getQueryStats() {
    return queryStats;
  }

  public void setQueryStats(Map<String, Object> queryStats) {
    this.queryStats = queryStats;
  }

  public void add(OResult item) {
    currentPage.add(item);
  }

  public void setExecutionPlan(OExecutionPlan executionPlan) {
    this.executionPlan = executionPlan;
  }
}
