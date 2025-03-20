package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OBinaryCompareOperator;
import com.orientechnologies.orient.core.sql.parser.OBinaryCondition;
import com.orientechnologies.orient.core.sql.parser.OBooleanExpression;
import com.orientechnologies.orient.core.sql.parser.OGeOperator;
import com.orientechnologies.orient.core.sql.parser.OGtOperator;
import com.orientechnologies.orient.core.sql.parser.OLeOperator;
import com.orientechnologies.orient.core.sql.parser.OLtOperator;
import com.orientechnologies.orient.core.sql.parser.ORid;
import java.util.Iterator;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class FetchFromClusterExecutionStep extends AbstractExecutionStep {

  public static final Object ORDER_ASC = "ASC";
  public static final Object ORDER_DESC = "DESC";
  private final QueryPlanningInfo queryPlanning;

  private int clusterId;
  private Object order;

  public FetchFromClusterExecutionStep(int clusterId) {
    this(clusterId, null);
  }

  public FetchFromClusterExecutionStep(int clusterId, QueryPlanningInfo queryPlanning) {
    super();
    this.clusterId = clusterId;
    this.queryPlanning = queryPlanning;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.start(ctx).close(ctx));
    long minClusterPosition = calculateMinClusterPosition();
    long maxClusterPosition = calculateMaxClusterPosition(ctx);
    ORecordIteratorCluster iterator =
        new ORecordIteratorCluster(
            (ODatabaseDocumentInternal) ctx.getDatabase(),
            clusterId,
            minClusterPosition,
            maxClusterPosition);
    Iterator<OIdentifiable> iter;
    if (ORDER_DESC.equals(order)) {
      iter = iterator.reversed();
    } else {
      iter = iterator;
    }

    OExecutionStream set = OExecutionStream.loadIterator(iter);

    set = set.interruptable();
    return set;
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

  private long calculateMaxClusterPosition(OCommandContext ctx) {
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
  public String prettyPrint(OPrintContext ctx) {
    String orderString = ORDER_DESC.equals(order) ? "DESC" : "ASC";
    String result =
        OExecutionStepInternal.getIndent(ctx)
            + "+ FETCH FROM CLUSTER "
            + clusterId
            + " "
            + orderString;
    if (ctx.isProfilingEnabled()) {
      result += " (" + ctx.getCostFormatted(this) + ")";
    }
    return result;
  }

  public void setOrder(Object order) {
    this.order = order;
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
  public OExecutionStepInternal copy(OCommandContext ctx) {
    FetchFromClusterExecutionStep result =
        new FetchFromClusterExecutionStep(
            this.clusterId, this.queryPlanning == null ? null : this.queryPlanning.copy());
    return result;
  }
}
