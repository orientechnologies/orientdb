package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStreamProducer;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.impl.ODatabaseDocumentDistributed;
import java.util.List;
import java.util.NoSuchElementException;

public final class OExecutionStreamDistributedFetch implements OExecutionStreamProducer {
  private final String queryId;
  private final ODatabaseDocumentDistributed db;
  private final String nodeName;
  private OExecutionStream current;
  private boolean closed = false;

  public OExecutionStreamDistributedFetch(
      String queryId, String nodeName, OExecutionStream first, ODatabaseDocumentDistributed db) {
    this.queryId = queryId;
    this.db = db;
    this.nodeName = nodeName;
    this.current = first;
  }

  @Override
  public OExecutionStream next(OCommandContext ctx) {
    if (!hasNext(ctx)) {
      throw new NoSuchElementException();
    }
    return current;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    if (closed) {
      return false;
    }
    if (current.hasNext(ctx)) {
      return true;
    } else {
      OFetchQueryPageTask task = new OFetchQueryPageTask(queryId);
      ODistributedResponse result = db.executeTaskOnNode(task, nodeName);
      current = OExecutionStream.resultIterator(((List<OResult>) result.getPayload()).iterator());
      return current.hasNext(ctx);
    }
  }

  @Override
  public void close(OCommandContext ctx) {
    OCloseQueryTask task = new OCloseQueryTask(queryId);
    db.executeTaskOnNode(task, nodeName);
    this.closed = true;
  }
}
