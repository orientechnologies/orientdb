package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.parser.OFromClause;
import com.orientechnologies.orient.core.sql.parser.OLimit;
import com.orientechnologies.orient.core.sql.parser.OProjection;
import com.orientechnologies.orient.core.sql.parser.OSelectStatement;
import com.orientechnologies.orient.core.sql.parser.OTimeout;
import com.orientechnologies.orient.core.sql.parser.OUpdateEdgeStatement;
import com.orientechnologies.orient.core.sql.parser.OUpdateOperations;
import com.orientechnologies.orient.core.sql.parser.OUpdateStatement;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorage.LOCKING_STRATEGY;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Created by luigidellaquila on 08/08/16. */
public class OUpdateExecutionPlanner {
  private final OFromClause target;
  public OWhereClause whereClause;

  protected boolean upsert = false;

  protected List<OUpdateOperations> operations = new ArrayList<OUpdateOperations>();
  protected boolean returnBefore = false;
  protected boolean returnAfter = false;
  protected boolean returnCount = false;

  protected boolean updateEdge = false;

  protected OProjection returnProjection;

  public OStorage.LOCKING_STRATEGY lockRecord = null;

  public OLimit limit;
  public OTimeout timeout;

  public OUpdateExecutionPlanner(OUpdateStatement oUpdateStatement) {
    if (oUpdateStatement instanceof OUpdateEdgeStatement) {
      updateEdge = true;
    }
    this.target = oUpdateStatement.getTarget().copy();
    this.whereClause =
        oUpdateStatement.getWhereClause() == null ? null : oUpdateStatement.getWhereClause().copy();
    if (oUpdateStatement.getOperations() == null) {
      this.operations = null;
    } else {
      this.operations =
          oUpdateStatement.getOperations().stream().map(x -> x.copy()).collect(Collectors.toList());
    }
    this.upsert = oUpdateStatement.isUpsert();

    this.returnBefore = oUpdateStatement.isReturnBefore();
    this.returnAfter = oUpdateStatement.isReturnAfter();
    this.returnCount = !(returnAfter || returnBefore);
    this.returnProjection =
        oUpdateStatement.getReturnProjection() == null
            ? null
            : oUpdateStatement.getReturnProjection().copy();
    this.lockRecord = oUpdateStatement.getLockRecord();
    this.limit = oUpdateStatement.getLimit() == null ? null : oUpdateStatement.getLimit().copy();
    this.timeout =
        oUpdateStatement.getTimeout() == null ? null : oUpdateStatement.getTimeout().copy();
  }

  public OUpdateExecutionPlan createExecutionPlan(OCommandContext ctx) {
    OUpdateExecutionPlan result = new OUpdateExecutionPlan();

    handleTarget(result, ctx, this.target, this.whereClause, this.timeout);
    // Issue #10913: fully materialize the source RID set before mutations begin so that an
    // index-driven range iterator cannot re-visit rows whose indexed value was pushed forward
    // by the UPDATE itself.
    result.chain(new AccumulateResultsStep());
    if (updateEdge) {
      result.chain(new CheckRecordTypeStep("E"));
    }
    handleUpsert(result, this.target, this.whereClause, this.upsert);
    handleTimeout(result, this.timeout);
    convertToModifiableResult(result);
    handleLimit(result, this.limit);
    handleReturnBefore(result, this.returnBefore);
    handleOperations(result, this.operations);
    handleSave(result, ctx);
    handleUnlock(result, this.lockRecord);
    handleResultForReturnBefore(result, this.returnBefore, returnProjection);
    handleResultForReturnAfter(result, this.returnAfter, returnProjection);
    handleResultForReturnCount(result, this.returnCount);
    return result;
  }

  /**
   * add a step that transforms a normal OResult in a specific object that under setProperty()
   * updates the actual OIdentifiable
   *
   * @param plan the execution plan
   */
  private void convertToModifiableResult(OUpdateExecutionPlan plan) {
    plan.chain(new ConvertToUpdatableResultStep());
  }

  private void handleResultForReturnCount(OUpdateExecutionPlan result, boolean returnCount) {
    if (returnCount) {
      result.chain(new CountStep());
    }
  }

  private void handleResultForReturnAfter(
      OUpdateExecutionPlan result, boolean returnAfter, OProjection returnProjection) {
    if (returnAfter) {
      // re-convert to normal step
      result.chain(new ConvertToResultInternalStep());
      if (returnProjection != null) {
        result.chain(new ProjectionCalculationStep(returnProjection));
      }
    }
  }

  private void handleResultForReturnBefore(
      OUpdateExecutionPlan result, boolean returnBefore, OProjection returnProjection) {
    if (returnBefore) {
      result.chain(new UnwrapPreviousValueStep());
      if (returnProjection != null) {
        result.chain(new ProjectionCalculationStep(returnProjection));
      }
    }
  }

  private void handleSave(OUpdateExecutionPlan result, OCommandContext ctx) {
    result.chain(new SaveElementStep());
  }

  private void handleTimeout(OUpdateExecutionPlan result, OTimeout timeout) {
    if (timeout != null && timeout.getVal().longValue() > 0) {
      result.chain(new TimeoutStep(timeout));
    }
  }

  private void handleReturnBefore(OUpdateExecutionPlan result, boolean returnBefore) {
    if (returnBefore) {
      result.chain(new CopyRecordContentBeforeUpdateStep());
    }
  }

  private void handleUnlock(OUpdateExecutionPlan result, LOCKING_STRATEGY lockRecord2) {
    if (lockRecord != null) {
      result.chain(new UnlockRecordStep(lockRecord));
    }
  }

  private void handleLimit(OUpdateExecutionPlan plan, OLimit limit) {
    if (limit != null) {
      plan.chain(new LimitExecutionStep(limit));
    }
  }

  private void handleUpsert(
      OUpdateExecutionPlan plan, OFromClause target, OWhereClause where, boolean upsert) {
    if (upsert) {
      plan.chain(new UpsertStep(target, where));
    }
  }

  private void handleOperations(OUpdateExecutionPlan plan, List<OUpdateOperations> ops) {
    if (ops != null) {
      for (OUpdateOperations op : ops) {
        switch (op.getType()) {
          case OUpdateOperations.TYPE_SET:
            plan.chain(new UpdateSetStep(op.getUpdateItems()));
            if (updateEdge) {
              plan.chain(new UpdateEdgePointersStep());
            }
            break;
          case OUpdateOperations.TYPE_REMOVE:
            plan.chain(new UpdateRemoveStep(op.getUpdateRemoveItems()));
            break;
          case OUpdateOperations.TYPE_MERGE:
            plan.chain(new UpdateMergeStep(op.getJson()));
            break;
          case OUpdateOperations.TYPE_CONTENT:
            plan.chain(new UpdateContentStep(op.getJson()));
            break;
          case OUpdateOperations.TYPE_PUT:
          case OUpdateOperations.TYPE_INCREMENT:
          case OUpdateOperations.TYPE_ADD:
            throw new OCommandExecutionException(
                "Cannot execute with UPDATE PUT/ADD/INCREMENT new executor: " + op);
        }
      }
    }
  }

  private void handleTarget(
      OUpdateExecutionPlan result,
      OCommandContext ctx,
      OFromClause target,
      OWhereClause whereClause,
      OTimeout timeout) {
    OSelectStatement sourceStatement = new OSelectStatement(-1);
    sourceStatement.setTarget(target);
    sourceStatement.setWhereClause(whereClause);
    if (timeout != null) {
      sourceStatement.setTimeout(this.timeout.copy());
    }
    sourceStatement.setLockRecord(lockRecord);
    OSelectExecutionPlanner planner = new OSelectExecutionPlanner(sourceStatement);
    result.chain(new SubQueryStep(planner.createExecutionPlan(ctx, false), ctx, ctx));
  }
}
