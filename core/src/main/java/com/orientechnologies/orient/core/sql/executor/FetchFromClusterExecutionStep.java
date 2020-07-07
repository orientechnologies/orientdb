package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OExecutionThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OCommandInterruptedException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.sql.parser.OBinaryCompareOperator;
import com.orientechnologies.orient.core.sql.parser.OBinaryCondition;
import com.orientechnologies.orient.core.sql.parser.OBooleanExpression;
import com.orientechnologies.orient.core.sql.parser.OGeOperator;
import com.orientechnologies.orient.core.sql.parser.OGtOperator;
import com.orientechnologies.orient.core.sql.parser.OLeOperator;
import com.orientechnologies.orient.core.sql.parser.OLtOperator;
import com.orientechnologies.orient.core.sql.parser.ORid;
import java.util.Map;
import java.util.Optional;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class FetchFromClusterExecutionStep extends AbstractExecutionStep {

  public static final Object ORDER_ASC = "ASC";
  public static final Object ORDER_DESC = "DESC";
  private final QueryPlanningInfo queryPlanning;

  private int clusterId;
  private Object order;

  private ORecordIteratorCluster iterator;
  private long cost = 0;

  public FetchFromClusterExecutionStep(
      int clusterId, OCommandContext ctx, boolean profilingEnabled) {
    this(clusterId, null, ctx, profilingEnabled);
  }

  public FetchFromClusterExecutionStep(
      int clusterId,
      QueryPlanningInfo queryPlanning,
      OCommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.clusterId = clusterId;
    this.queryPlanning = queryPlanning;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      if (iterator == null) {
        long minClusterPosition = calculateMinClusterPosition();
        long maxClusterPosition = calculateMaxClusterPosition();
        iterator =
            new ORecordIteratorCluster(
                (ODatabaseDocumentInternal) ctx.getDatabase(),
                clusterId,
                minClusterPosition,
                maxClusterPosition);
        if (ORDER_DESC.equals(order)) {
          iterator.last();
        }
      }
      OResultSet rs =
          new OResultSet() {

            private int nFetched = 0;

            @Override
            public boolean hasNext() {
              if (timedOut) {
                throw new OTimeoutException("Command execution timeout");
              }
              long begin = profilingEnabled ? System.nanoTime() : 0;
              try {
                if (nFetched >= nRecords) {
                  return false;
                }
                if (ORDER_DESC.equals(order)) {
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
              if (timedOut) {
                throw new OTimeoutException("Command execution timeout");
              }

              if (nFetched % 100 == 0 && OExecutionThreadLocal.isInterruptCurrentOperation()) {
                throw new OCommandInterruptedException("The command has been interrupted");
              }
              long begin = profilingEnabled ? System.nanoTime() : 0;
              try {
                if (nFetched >= nRecords) {
                  throw new IllegalStateException();
                }
                if (ORDER_DESC.equals(order) && !iterator.hasPrevious()) {
                  throw new IllegalStateException();
                } else if (!ORDER_DESC.equals(order) && !iterator.hasNext()) {
                  throw new IllegalStateException();
                }

                ORecord record = null;
                if (ORDER_DESC.equals(order)) {
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
            public void close() {}

            @Override
            public Optional<OExecutionPlan> getExecutionPlan() {
              return Optional.empty();
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

  private long calculateMinClusterPosition() {
    if (queryPlanning == null
        || queryPlanning.ridRangeConditions == null
        || queryPlanning.ridRangeConditions.isEmpty()) {
      return -1;
    }

    long maxValue = -1;

    for (OBooleanExpression ridRangeCondition : queryPlanning.ridRangeConditions.getSubBlocks()) {
      if (ridRangeCondition instanceof OBinaryCondition) {
        OBinaryCondition cond = (OBinaryCondition) ridRangeCondition;
        ORid condRid = cond.getRight().getRid();
        OBinaryCompareOperator operator = cond.getOperator();
        if (condRid != null) {
          if (condRid.getCluster().getValue().intValue() != this.clusterId) {
            continue;
          }
          if (operator instanceof OGtOperator || operator instanceof OGeOperator) {
            maxValue = Math.max(maxValue, condRid.getPosition().getValue().longValue());
          }
        }
      }
    }

    return maxValue;
  }

  private long calculateMaxClusterPosition() {
    if (queryPlanning == null
        || queryPlanning.ridRangeConditions == null
        || queryPlanning.ridRangeConditions.isEmpty()) {
      return -1;
    }
    long minValue = Long.MAX_VALUE;

    for (OBooleanExpression ridRangeCondition : queryPlanning.ridRangeConditions.getSubBlocks()) {
      if (ridRangeCondition instanceof OBinaryCondition) {
        OBinaryCondition cond = (OBinaryCondition) ridRangeCondition;
        ORID conditionRid;

        Object obj;
        if (((OBinaryCondition) ridRangeCondition).getRight().getRid() != null) {
          obj =
              ((OBinaryCondition) ridRangeCondition)
                  .getRight()
                  .getRid()
                  .toRecordId((OResult) null, ctx);
        } else {
          obj = ((OBinaryCondition) ridRangeCondition).getRight().execute((OResult) null, ctx);
        }

        conditionRid = ((OIdentifiable) obj).getIdentity();
        OBinaryCompareOperator operator = cond.getOperator();
        if (conditionRid != null) {
          if (conditionRid.getClusterId() != this.clusterId) {
            continue;
          }
          if (operator instanceof OLtOperator || operator instanceof OLeOperator) {
            minValue = Math.min(minValue, conditionRid.getClusterPosition());
          }
        }
      }
    }

    return minValue == Long.MAX_VALUE ? -1 : minValue;
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
    String orderString = ORDER_DESC.equals(order) ? "DESC" : "ASC";
    String result =
        OExecutionStepInternal.getIndent(depth, indent)
            + "+ FETCH FROM CLUSTER "
            + clusterId
            + " "
            + orderString;
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

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    FetchFromClusterExecutionStep result =
        new FetchFromClusterExecutionStep(
            this.clusterId,
            this.queryPlanning == null ? null : this.queryPlanning.copy(),
            ctx,
            profilingEnabled);
    return result;
  }
}
