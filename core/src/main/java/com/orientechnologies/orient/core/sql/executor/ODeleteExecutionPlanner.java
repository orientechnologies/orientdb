package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexCandidate;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexCandidateOne;
import com.orientechnologies.orient.core.sql.executor.metadata.OSpecificIndexFinder;
import com.orientechnologies.orient.core.sql.parser.OAndBlock;
import com.orientechnologies.orient.core.sql.parser.OBooleanExpression;
import com.orientechnologies.orient.core.sql.parser.ODeleteStatement;
import com.orientechnologies.orient.core.sql.parser.OFromClause;
import com.orientechnologies.orient.core.sql.parser.OIndexIdentifier;
import com.orientechnologies.orient.core.sql.parser.OLimit;
import com.orientechnologies.orient.core.sql.parser.OSelectStatement;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;
import java.util.Optional;

/** Created by luigidellaquila on 08/08/16. */
public class ODeleteExecutionPlanner {

  private final OFromClause fromClause;
  private final OWhereClause whereClause;
  private final boolean returnBefore;
  private final OLimit limit;
  private final boolean unsafe;

  public ODeleteExecutionPlanner(ODeleteStatement stm) {
    this.fromClause = stm.getFromClause() == null ? null : stm.getFromClause().copy();
    this.whereClause = stm.getWhereClause() == null ? null : stm.getWhereClause().copy();
    this.returnBefore = stm.isReturnBefore();
    this.limit = stm.getLimit() == null ? null : stm.getLimit();
    this.unsafe = stm.isUnsafe();
  }

  public ODeleteExecutionPlan createExecutionPlan(OCommandContext ctx) {
    ODeleteExecutionPlan result = new ODeleteExecutionPlan();

    if (handleIndexAsTarget(result, fromClause.getItem().getIndex(), whereClause, ctx)) {
      if (limit != null) {
        throw new OCommandExecutionException("Cannot apply a LIMIT on a delete from index");
      }
      if (unsafe) {
        throw new OCommandExecutionException("Cannot apply a UNSAFE on a delete from index");
      }
      if (returnBefore) {
        throw new OCommandExecutionException("Cannot apply a RETURN BEFORE on a delete from index");
      }

      handleReturn(result, this.returnBefore);
    } else {
      handleTarget(result, ctx, this.fromClause, this.whereClause);
      handleUnsafe(result, this.unsafe);
      handleLimit(result, this.limit);
      handleDelete(result);
      handleReturn(result, this.returnBefore);
    }
    return result;
  }

  private boolean handleIndexAsTarget(
      ODeleteExecutionPlan result,
      OIndexIdentifier indexIdentifier,
      OWhereClause whereClause,
      OCommandContext ctx) {
    if (indexIdentifier == null) {
      return false;
    }
    String indexName = indexIdentifier.getIndexName();
    final ODatabaseDocumentInternal database = (ODatabaseDocumentInternal) ctx.getDatabase();
    OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, indexName);
    if (index == null) {
      throw new OCommandExecutionException("Index not found: " + indexName);
    }

    switch (indexIdentifier.getType()) {
      case INDEX:
        OBooleanExpression ridCondition = null;
        Optional<OIndexCandidate> found;
        if (whereClause == null || whereClause.isEmpty()) {
          if (!index.supportsOrderedIterations()) {
            throw new OCommandExecutionException(
                "Index " + indexName + " does not allow iteration without a condition");
          }
          found = Optional.empty();
        } else {
          ridCondition = whereClause.getIndexRidCondition();
          found = whereClause.findIndex(new OSpecificIndexFinder(index), ctx);
          found = found.flatMap((x) -> x.normalize(ctx)).flatMap((x) -> x.finalize(ctx));
          if (found.isEmpty()) {
            throw new OCommandExecutionException(
                "Index queries with this kind of condition are not supported yet: " + whereClause);
          }
        }

        OIndexCandidate candidate = found.orElseGet(() -> new OIndexCandidateOne(index.getName()));
        result.chain(new DeleteFromIndexStep(index, candidate, ridCondition));
        if (ridCondition != null) {
          OWhereClause where = new OWhereClause(-1);
          where.setBaseExpression(ridCondition);
          result.chain(new FilterStep(where, -1, false));
        }
        return true;
      case VALUES:
      case VALUESASC:
        if (!index.supportsOrderedIterations()) {
          throw new OCommandExecutionException(
              "Index " + indexName + " does not allow iteration on values");
        }
        result.chain(
            new FetchFromIndexValuesStep(new OIndexCandidateOne(index.getName()), true, ctx));
        result.chain(new GetValueFromIndexEntryStep(null));
        break;
      case VALUESDESC:
        if (!index.supportsOrderedIterations()) {
          throw new OCommandExecutionException(
              "Index " + indexName + " does not allow iteration on values");
        }
        result.chain(
            new FetchFromIndexValuesStep(new OIndexCandidateOne(index.getName()), false, ctx));
        result.chain(new GetValueFromIndexEntryStep(null));
        break;
    }
    return false;
  }

  private void handleDelete(ODeleteExecutionPlan result) {
    result.chain(new DeleteStep());
  }

  private void handleUnsafe(ODeleteExecutionPlan result, boolean unsafe) {
    if (!unsafe) {
      result.chain(new CheckSafeDeleteStep());
    }
  }

  private void handleReturn(ODeleteExecutionPlan result, boolean returnBefore) {
    if (!returnBefore) {
      result.chain(new CountStep());
    }
  }

  private void handleLimit(OUpdateExecutionPlan plan, OLimit limit) {
    if (limit != null) {
      plan.chain(new LimitExecutionStep(limit));
    }
  }

  private void handleTarget(
      OUpdateExecutionPlan result,
      OCommandContext ctx,
      OFromClause target,
      OWhereClause whereClause) {
    OSelectStatement sourceStatement = new OSelectStatement(-1);
    sourceStatement.setTarget(target);
    sourceStatement.setWhereClause(whereClause);
    OSelectExecutionPlanner planner = new OSelectExecutionPlanner(sourceStatement);
    result.chain(new SubQueryStep(planner.createExecutionPlan(ctx, false), ctx, ctx));
  }

  private OBooleanExpression getKeyCondition(OAndBlock andBlock) {
    for (OBooleanExpression exp : andBlock.getSubBlocks()) {
      String str = exp.toString();
      if (str.length() < 5) {
        continue;
      }
      if (str.substring(0, 4).equalsIgnoreCase("key ")) {
        return exp;
      }
    }
    return null;
  }

  private OBooleanExpression getRidCondition(OAndBlock andBlock) {
    for (OBooleanExpression exp : andBlock.getSubBlocks()) {
      String str = exp.toString();
      if (str.length() < 5) {
        continue;
      }
      if (str.substring(0, 4).equalsIgnoreCase("rid ")) {
        return exp;
      }
    }
    return null;
  }
}
