package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.record.ORecord;

import java.util.Map;
import java.util.Optional;

/**
 * @author Luigi Dell'Aquila
 */
public class FetchFromClusterExecutionStep extends AbstractExecutionStep {

  public static final Object ORDER_ASC  = "ASC";
  public static final Object ORDER_DESC = "DESC";

  private final int                    clusterId;
  private       ORecordIteratorCluster iterator;
  private       Object                 order;

  public FetchFromClusterExecutionStep(int clusterId, OCommandContext ctx) {
    super(ctx);
    this.clusterId = clusterId;
  }

  @Override public OTodoResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    if (iterator == null) {
      iterator = new ORecordIteratorCluster((ODatabaseDocumentInternal) ctx.getDatabase(),
          (ODatabaseDocumentInternal) ctx.getDatabase(), clusterId);
      if (ORDER_DESC == order) {
        iterator.last();
      }
    }
    OTodoResultSet rs = new OTodoResultSet() {

      int nFetched = 0;

      @Override public boolean hasNext() {
        if (nFetched >= nRecords) {
          return false;
        }
        if (ORDER_DESC == order) {
          return iterator.hasPrevious();
        } else {
          return iterator.hasNext();
        }
      }

      @Override public OResult next() {
        if (nFetched >= nRecords) {
          throw new IllegalStateException();
        }
        if (ORDER_DESC == order && !iterator.hasPrevious()) {
          throw new IllegalStateException();
        } else if (ORDER_DESC != order && !iterator.hasNext()) {
          throw new IllegalStateException();
        }

        ORecord record = null;
        if (ORDER_DESC == order) {
          record = iterator.previous();
        } else {
          record = iterator.next();
        }
        nFetched++;
        OResultInternal result = new OResultInternal();
        result.element = record;
        ctx.setVariable("$current", result);
        return result;
      }

      @Override public void close() {

      }

      @Override public Optional<OExecutionPlan> getExecutionPlan() {
        return null;
      }

      @Override public Map<String, Object> getQueryStats() {
        return null;
      }
    };
    return rs;
  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override public void sendTimeout() {
    super.sendTimeout();
  }

  @Override public void close() {
    super.close();
  }

  @Override public void sendResult(Object o, Status status) {

  }

  @Override public String prettyPrint(int depth, int indent) {
    return OExecutionStepInternal.getIndent(depth, indent) + "+ FETCH FROM CLUSTER " + clusterId + " " + (ORDER_DESC.equals(order) ?
        "DESC" :
        "ASC");
  }

  public void setOrder(Object order) {
    this.order = order;
  }
}
