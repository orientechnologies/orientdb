package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.record.ORecord;

import java.util.Map;
import java.util.Optional;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class FetchFromClusterExecutionStep extends AbstractExecutionStep {

  public static final Object ORDER_ASC  = "ASC";
  public static final Object ORDER_DESC = "DESC";

  private int    clusterId;
  private Object order;

  private ORecordIteratorCluster iterator;
  private long cost = 0;

  public FetchFromClusterExecutionStep(int clusterId, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.clusterId = clusterId;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      if (iterator == null) {
        iterator = new ORecordIteratorCluster((ODatabaseDocumentInternal) ctx.getDatabase(),
            (ODatabaseDocumentInternal) ctx.getDatabase(), clusterId);
        if (ORDER_DESC == order) {
          iterator.last();
        }
      }
      OResultSet rs = new OResultSet() {

        int nFetched = 0;

        @Override
        public boolean hasNext() {
          long begin = profilingEnabled ? System.nanoTime() : 0;
          try {
            if (nFetched >= nRecords) {
              return false;
            }
            if (ORDER_DESC == order) {
              return iterator.hasPrevious();
            } else {
              return iterator.hasNext();
            }
          } finally {
            if (profilingEnabled) {
              cost += (System.nanoTime() - begin);
            }
          }
        }

        @Override
        public OResult next() {
          long begin = profilingEnabled ? System.nanoTime() : 0;
          try {
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
          } finally {
            if (profilingEnabled) {
              cost += (System.nanoTime() - begin);
            }
          }
        }

        @Override
        public void close() {

        }

        @Override
        public Optional<OExecutionPlan> getExecutionPlan() {
          return null;
        }

        @Override
        public Map<String, Long> getQueryStats() {
          return null;
        }

      };
      return rs;
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }

  }

  @Override
  public void sendTimeout() {
    super.sendTimeout();
  }

  @Override
  public void close() {
    super.close();
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String result =
        OExecutionStepInternal.getIndent(depth, indent) + "+ FETCH FROM CLUSTER " + clusterId + " " + (ORDER_DESC.equals(order) ?
            "DESC" :
            "ASC");
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

  public void setOrder(Object order) {
    this.order = order;
  }

  @Override
  public long getCost() {
    return cost;
  }

  @Override
  public OResult serialize() {
    OResultInternal result = OExecutionStepInternal.basicSerialize(this);
    result.setProperty("clusterId", clusterId);
    result.setProperty("order", order);
    return result;
  }

  @Override
  public void deserialize(OResult fromResult) {
    try {
      OExecutionStepInternal.basicDeserialize(fromResult, this);
      this.clusterId = fromResult.getProperty("clusterId");
      Object orderProp = fromResult.getProperty("order");
      if (orderProp != null) {
        this.order = ORDER_ASC.equals(fromResult.getProperty("order")) ? ORDER_ASC : ORDER_DESC;
      }
    } catch (Exception e) {
      throw OException.wrapException(new OCommandExecutionException(""), e);
    }
  }
}
