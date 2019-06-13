package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.sql.parser.*;

import java.util.*;

/**
 * Created by luigidellaquila on 11/08/16.
 */
public class DeleteFromIndexStep extends AbstractExecutionStep {
  protected final OIndex             index;
  private final   OBinaryCondition   additional;
  private final   OBooleanExpression ridCondition;
  private final   boolean            orderAsc;

  Map.Entry<Object, OIdentifiable> nextEntry = null;

  OBooleanExpression condition;

  private boolean inited = false;
  private OIndexCursor cursor;

  private long cost = 0;

  public DeleteFromIndexStep(OIndex<?> index, OBooleanExpression condition, OBinaryCondition additionalRangeCondition,
      OBooleanExpression ridCondition, OCommandContext ctx, boolean profilingEnabled) {
    this(index, condition, additionalRangeCondition, ridCondition, true, ctx, profilingEnabled);
  }

  public DeleteFromIndexStep(OIndex<?> index, OBooleanExpression condition, OBinaryCondition additionalRangeCondition,
      OBooleanExpression ridCondition, boolean orderAsc, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.index = index;
    this.condition = condition;
    this.additional = additionalRangeCondition;
    this.ridCondition = ridCondition;
    this.orderAsc = orderAsc;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    init();

    return new OResultSet() {
      int localCount = 0;

      @Override
      public boolean hasNext() {
        return (localCount < nRecords && nextEntry != null);
      }

      @Override
      public OResult next() {
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {
          if (!hasNext()) {
            throw new IllegalStateException();
          }
          Map.Entry<Object, OIdentifiable> entry = nextEntry;
          OResultInternal result = new OResultInternal();
          OIdentifiable value = entry.getValue();

//        if(index.isAutomatic()) {
//          ORecord record = value.getRecord();
//          if (record != null) {
//            ctx.getDatabase().delete(record);
//          }
//        }else{
          index.remove(entry.getKey(), value);
//        }
          localCount++;
          nextEntry = loadNextEntry(ctx);
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
        return Optional.empty();
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };
  }

  private synchronized void init() {
    if (inited) {
      return;
    }
    inited = true;
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      init(condition);
      nextEntry = loadNextEntry(ctx);
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
  }

  private Map.Entry<Object, OIdentifiable> loadNextEntry(OCommandContext commandContext) {
    Map.Entry<Object, OIdentifiable> result = cursor.nextEntry();
    while (true) {
      if (result == null) {
        return null;
      }
      if (ridCondition == null) {
        return result;
      }
      OResultInternal res = new OResultInternal();
      res.setProperty("rid", result.getValue());
      if (ridCondition.evaluate(res, commandContext)) {
        return result;
      }
      result = cursor.nextEntry();
    }
  }

  private void init(OBooleanExpression condition) {
    if (index.getDefinition() == null) {
      return;
    }
    if (condition == null) {
      processFlatIteration();
    } else if (condition instanceof OBinaryCondition) {
      processBinaryCondition();
    } else if (condition instanceof OBetweenCondition) {
      processBetweenCondition();
    } else if (condition instanceof OAndBlock) {
      processAndBlock();
    } else {
      throw new OCommandExecutionException("search for index for " + condition + " is not supported yet");
    }
  }

  /**
   * it's not key = [...] but a real condition on field names, already ordered (field names will be ignored)
   */
  private void processAndBlock() {
    OCollection fromKey = indexKeyFrom((OAndBlock) condition, additional);
    OCollection toKey = indexKeyTo((OAndBlock) condition, additional);
    boolean fromKeyIncluded = indexKeyFromIncluded((OAndBlock) condition, additional);
    boolean toKeyIncluded = indexKeyToIncluded((OAndBlock) condition, additional);
    init(fromKey, fromKeyIncluded, toKey, toKeyIncluded);
  }

  private void processFlatIteration() {
    cursor = isOrderAsc() ? index.cursor() : index.descCursor();
  }

  private void init(OCollection fromKey, boolean fromKeyIncluded, OCollection toKey, boolean toKeyIncluded) {
    Object secondValue = fromKey.execute((OResult) null, ctx);
    Object thirdValue = toKey.execute((OResult) null, ctx);
    OIndexDefinition indexDef = index.getDefinition();
    if (index.supportsOrderedIterations()) {
      cursor = index
          .iterateEntriesBetween(toBetweenIndexKey(indexDef, secondValue), fromKeyIncluded, toBetweenIndexKey(indexDef, thirdValue),
              toKeyIncluded, isOrderAsc());
    } else if (additional == null && allEqualities((OAndBlock) condition)) {
      cursor = index.iterateEntries(toIndexKey(indexDef, secondValue), isOrderAsc());
    } else {
      throw new UnsupportedOperationException("Cannot evaluate " + this.condition + " on index " + index);
    }
  }

  private boolean allEqualities(OAndBlock condition) {
    if (condition == null) {
      return false;
    }
    for (OBooleanExpression exp : condition.getSubBlocks()) {
      if (exp instanceof OBinaryCondition) {
        if (((OBinaryCondition) exp).getOperator() instanceof OEqualsCompareOperator) {
          return true;
        }
      } else {
        return false;
      }
    }
    return true;
  }

  private void processBetweenCondition() {
    OIndexDefinition definition = index.getDefinition();
    OExpression key = ((OBetweenCondition) condition).getFirst();
    if (!key.toString().equalsIgnoreCase("key")) {
      throw new OCommandExecutionException("search for index for " + condition + " is not supported yet");
    }
    OExpression second = ((OBetweenCondition) condition).getSecond();
    OExpression third = ((OBetweenCondition) condition).getThird();

    Object secondValue = second.execute((OResult) null, ctx);
    Object thirdValue = third.execute((OResult) null, ctx);
    cursor = index
        .iterateEntriesBetween(toBetweenIndexKey(definition, secondValue), true, toBetweenIndexKey(definition, thirdValue), true,
            isOrderAsc());
  }

  private void processBinaryCondition() {
    OIndexDefinition definition = index.getDefinition();
    OBinaryCompareOperator operator = ((OBinaryCondition) condition).getOperator();
    OExpression left = ((OBinaryCondition) condition).getLeft();
    if (!left.toString().equalsIgnoreCase("key")) {
      throw new OCommandExecutionException("search for index for " + condition + " is not supported yet");
    }
    Object rightValue = ((OBinaryCondition) condition).getRight().execute((OResult) null, ctx);
    cursor = createCursor(operator, definition, rightValue, ctx);
  }

  private Collection toIndexKey(OIndexDefinition definition, Object rightValue) {
    if (definition.getFields().size() == 1 && rightValue instanceof Collection) {
      rightValue = ((Collection) rightValue).iterator().next();
    }
    if (rightValue instanceof List) {
      rightValue = definition.createValue((List<?>) rightValue);
    } else {
      rightValue = definition.createValue(rightValue);
    }
    if (!(rightValue instanceof Collection)) {
      rightValue = Collections.singleton(rightValue);
    }
    return (Collection) rightValue;
  }

  private Object toBetweenIndexKey(OIndexDefinition definition, Object rightValue) {
    if (definition.getFields().size() == 1 && rightValue instanceof Collection) {
      rightValue = ((Collection) rightValue).iterator().next();
    }
    rightValue = definition.createValue(rightValue);

    if (definition.getFields().size() > 1 && !(rightValue instanceof Collection)) {
      rightValue = Collections.singleton(rightValue);
    }
    return rightValue;
  }

  private OIndexCursor createCursor(OBinaryCompareOperator operator, OIndexDefinition definition, Object value,
      OCommandContext ctx) {
    boolean orderAsc = isOrderAsc();
    if (operator instanceof OEqualsCompareOperator) {
      return index.iterateEntries(toIndexKey(definition, value), orderAsc);
    } else if (operator instanceof OGeOperator) {
      return index.iterateEntriesMajor(value, true, orderAsc);
    } else if (operator instanceof OGtOperator) {
      return index.iterateEntriesMajor(value, false, orderAsc);
    } else if (operator instanceof OLeOperator) {
      return index.iterateEntriesMinor(value, true, orderAsc);
    } else if (operator instanceof OLtOperator) {
      return index.iterateEntriesMinor(value, false, orderAsc);
    } else {
      throw new OCommandExecutionException("search for index for " + condition + " is not supported yet");
    }

  }

  protected boolean isOrderAsc() {
    return orderAsc;
  }

  private OCollection indexKeyFrom(OAndBlock keyCondition, OBinaryCondition additional) {
    OCollection result = new OCollection(-1);
    for (OBooleanExpression exp : keyCondition.getSubBlocks()) {
      if (exp instanceof OBinaryCondition) {
        OBinaryCondition binaryCond = ((OBinaryCondition) exp);
        OBinaryCompareOperator operator = binaryCond.getOperator();
        if ((operator instanceof OEqualsCompareOperator) || (operator instanceof OGtOperator)
            || (operator instanceof OGeOperator)) {
          result.add(binaryCond.getRight());
        } else if (additional != null) {
          result.add(additional.getRight());
        }
      } else {
        throw new UnsupportedOperationException("Cannot execute index query with " + exp);
      }
    }
    return result;
  }

  private OCollection indexKeyTo(OAndBlock keyCondition, OBinaryCondition additional) {
    OCollection result = new OCollection(-1);
    for (OBooleanExpression exp : keyCondition.getSubBlocks()) {
      if (exp instanceof OBinaryCondition) {
        OBinaryCondition binaryCond = ((OBinaryCondition) exp);
        OBinaryCompareOperator operator = binaryCond.getOperator();
        if ((operator instanceof OEqualsCompareOperator) || (operator instanceof OLtOperator)
            || (operator instanceof OLeOperator)) {
          result.add(binaryCond.getRight());
        } else if (additional != null) {
          result.add(additional.getRight());
        }
      } else {
        throw new UnsupportedOperationException("Cannot execute index query with " + exp);
      }
    }
    return result;
  }

  private boolean indexKeyFromIncluded(OAndBlock keyCondition, OBinaryCondition additional) {
    OBooleanExpression exp = keyCondition.getSubBlocks().get(keyCondition.getSubBlocks().size() - 1);
    if (exp instanceof OBinaryCondition) {
      OBinaryCompareOperator operator = ((OBinaryCondition) exp).getOperator();
      OBinaryCompareOperator additionalOperator = additional == null ? null : ((OBinaryCondition) additional).getOperator();
      if (isGreaterOperator(operator)) {
        if (isIncludeOperator(operator)) {
          return true;
        } else {
          return false;
        }
      } else if (additionalOperator == null || (isIncludeOperator(additionalOperator) && isGreaterOperator(additionalOperator))) {
        return true;
      } else {
        return false;
      }
    } else {
      throw new UnsupportedOperationException("Cannot execute index query with " + exp);
    }
  }

  private boolean isGreaterOperator(OBinaryCompareOperator operator) {
    if (operator == null) {
      return false;
    }
    return operator instanceof OGeOperator || operator instanceof OGtOperator;
  }

  private boolean isLessOperator(OBinaryCompareOperator operator) {
    if (operator == null) {
      return false;
    }
    return operator instanceof OLeOperator || operator instanceof OLtOperator;
  }

  private boolean isIncludeOperator(OBinaryCompareOperator operator) {
    if (operator == null) {
      return false;
    }
    return operator instanceof OGeOperator || operator instanceof OLeOperator;
  }

  private boolean indexKeyToIncluded(OAndBlock keyCondition, OBinaryCondition additional) {
    OBooleanExpression exp = keyCondition.getSubBlocks().get(keyCondition.getSubBlocks().size() - 1);
    if (exp instanceof OBinaryCondition) {
      OBinaryCompareOperator operator = ((OBinaryCondition) exp).getOperator();
      OBinaryCompareOperator additionalOperator = additional == null ? null : ((OBinaryCondition) additional).getOperator();
      if (isLessOperator(operator)) {
        if (isIncludeOperator(operator)) {
          return true;
        } else {
          return false;
        }
      } else if (additionalOperator == null || (isIncludeOperator(additionalOperator) && isLessOperator(additionalOperator))) {
        return true;
      } else {
        return false;
      }
    } else {
      throw new UnsupportedOperationException("Cannot execute index query with " + exp);
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String result = OExecutionStepInternal.getIndent(depth, indent) + "+ DELETE FROM INDEX " + index.getName();
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    result += (condition == null ?
        "" :
        ("\n" + OExecutionStepInternal.getIndent(depth, indent) + "  " + condition + (additional == null ?
            "" :
            " and " + additional)));
    return result;
  }

  @Override
  public long getCost() {
    return cost;
  }
}
