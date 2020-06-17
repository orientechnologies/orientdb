package com.orientechnologies.orient.server.distributed.impl.sql.executor;

import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import com.orientechnologies.orient.server.distributed.impl.task.OCloseQueryTask;
import com.orientechnologies.orient.server.distributed.impl.task.OFetchQueryPageTask;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

/** Created by luigidellaquila on 21/06/17. */
public class ODistributedResultSet implements OResultSet {
  private String queryId;
  private List<OResult> data;
  private ODatabaseDocumentDistributed database;
  private String nodeName;

  private int nextItem = -1;
  private boolean finished = false;

  @Override
  public boolean hasNext() {
    if (finished) {
      return false;
    }
    if (nextItem < 0) {
      throw new IllegalStateException();
    }
    if (data.size() < nextItem) {
      return true;
    }
    fetchNextBlock();
    if (data.size() < nextItem) {
      return true;
    }
    return false;
  }

  @Override
  public OResult next() {
    if (nextItem < 0) {
      throw new IllegalStateException();
    }
    if (finished) {
      throw new NoSuchElementException();
    }
    if (data.size() < nextItem) {
      return data.get(nextItem++);
    }
    fetchNextBlock();
    if (finished || data.size() >= nextItem) {
      throw new NoSuchElementException();
    }
    return data.get(nextItem++);
  }

  private void fetchNextBlock() {
    OFetchQueryPageTask task = new OFetchQueryPageTask(queryId);
    ODistributedResponse result = database.executeTaskOnNode(task, nodeName);
    setData((List<OResult>) result.getPayload());
  }

  @Override
  public void close() {
    OCloseQueryTask task = new OCloseQueryTask(queryId);
    database.executeTaskOnNode(task, nodeName);
    this.finished = true;
  }

  @Override
  public Optional<OExecutionPlan> getExecutionPlan() {
    return null;
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return null;
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  public void setData(List<OResult> data) {
    this.data = data;
    nextItem = 0;
    finished = data.size() == 0;
  }

  public void setDatabase(ODatabaseDocumentDistributed database) {
    this.database = database;
  }

  public void setNodeName(String nodeName) {
    this.nodeName = nodeName;
  }
}
