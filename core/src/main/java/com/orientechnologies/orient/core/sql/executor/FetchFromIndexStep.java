package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.parser.*;

import java.util.*;

/**
 * Created by luigidellaquila on 23/07/16.
 */
public class FetchFromIndexStep extends AbstractExecutionStep {
  protected OIndex             index;
  protected OBooleanExpression condition;
  private   OBinaryCondition   additionalRangeCondition;

  private boolean orderAsc;

  protected String indexName;//for distributed serialization only

  private long cost  = 0;
  private long count = 0;

  private boolean inited = false;
  private OIndexCursor cursor;
  OMultiCollectionIterator<Map.Entry<Object, OIdentifiable>> customIterator;
  private Iterator nullKeyIterator;
  private Map.Entry<Object, OIdentifiable> nextEntry = null;

  public FetchFromIndexStep(OIndex<?> index, OBooleanExpression condition, OBinaryCondition additionalRangeCondition,
      OCommandContext ctx, boolean profilingEnabled) {
    this(index, condition, additionalRangeCondition, true, ctx, profilingEnabled);
  }

  public FetchFromIndexStep(OIndex<?> index, OBooleanExpression condition, OBinaryCondition additionalRangeCondition,
      boolean orderAsc, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.index = index;
    this.condition = condition;
    this.additionalRangeCondition = additionalRangeCondition;
    this.orderAsc = orderAsc;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    init(ctx.getDatabase());
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    return new OResultSet() {
      int localCount = 0;

      @Override
      public boolean hasNext() {
        return (localCount < nRecords && nextEntry != null);
      }

      @Override
      public OResult next() {
        if (!hasNext()) {
          throw new IllegalStateException();
        }
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {
          Map.Entry<Object, OIdentifiable> currentEntry = nextEntry;
          fetchNextEntry();

          localCount++;
          OResultInternal result = new OResultInternal();
          result.setProperty("key", currentEntry.getKey());
          result.setProperty("rid", currentEntry.getValue());
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
  }

  private void fetchNextEntry() {
    nextEntry = null;
    if (cursor != null) {
      nextEntry = cursor.nextEntry();
    }
    if (nextEntry == null && customIterator != null && customIterator.hasNext()) {
      nextEntry = customIterator.next();
    }

    if (nextEntry == null && nullKeyIterator != null && nullKeyIterator.hasNext()) {
      OIdentifiable nextValue = (OIdentifiable) nullKeyIterator.next();
      nextEntry = new Map.Entry<Object, OIdentifiable>() {
        @Override
        public Object getKey() {
          return null;
        }

        @Override
        public OIdentifiable getValue() {
          return nextValue;
        }

        @Override
        public OIdentifiable setValue(OIdentifiable value) {
          return null;
        }
      };
    }
    if (nextEntry == null) {
      updateIndexStats();
    } else {
      count++;
    }
  }

  private void updateIndexStats() {
    //stats
    OQueryStats stats = OQueryStats.get((ODatabaseDocumentInternal) ctx.getDatabase());
    String indexName = index.getName();
    boolean range = false;
    int size = 0;

    if (condition == null) {
      size = 0;
    } else if (condition instanceof OBinaryCondition) {
      size = 1;
    } else if (condition instanceof OBetweenCondition) {
      size = 1;
      range = true;
    } else if (condition instanceof OAndBlock) {
      OAndBlock andBlock = ((OAndBlock) condition);
      size = andBlock.getSubBlocks().size();
      OBooleanExpression lastOp = andBlock.getSubBlocks().get(andBlock.getSubBlocks().size() - 1);
      if (lastOp instanceof OBinaryCondition) {
        OBinaryCompareOperator op = ((OBinaryCondition) lastOp).getOperator();
        range = op.isRangeOperator();
      }
    } else if (condition instanceof OInCondition) {
      size = 1;
    }
    stats.pushIndexStats(indexName, size, range, additionalRangeCondition != null, count);
  }

  private synchronized void init(ODatabase db) {
    if (inited) {
      return;
    }
    inited = true;
    init(condition, db);
  }

  private void init(OBooleanExpression condition, ODatabase db) {
    long begin = profilingEnabled ? System.nanoTime() : 0;
    if (index == null) {
      index = db.getMetadata().getIndexManager().getIndex(indexName);
    }
    try {
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
      } else if (condition instanceof OInCondition) {
        //TODO
        processInCondition();

      } else {
        throw new OCommandExecutionException("search for index for " + condition + " is not supported yet");
      }
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
  }

  private void processInCondition() {
    OIndexDefinition definition = index.getDefinition();
    OInCondition inCondition = (OInCondition) condition;

    OExpression left = inCondition.getLeft();
    if (!left.toString().equalsIgnoreCase("key")) {
      throw new OCommandExecutionException("search for index for " + condition + " is not supported yet");
    }
    Object rightValue = inCondition.evaluateRight((OResult) null, ctx);
    OEqualsCompareOperator equals = new OEqualsCompareOperator(-1);
    if (OMultiValue.isMultiValue(rightValue)) {
      customIterator = new OMultiCollectionIterator<>();
      for (Object item : OMultiValue.getMultiValueIterable(rightValue)) {
        OIndexCursor localCursor = createCursor(equals, definition, item, ctx);

        customIterator.add(new Iterator<Map.Entry>() {
          @Override
          public boolean hasNext() {
            return localCursor.hasNext();
          }

          @Override
          public Map.Entry next() {
            if (!localCursor.hasNext()) {
              throw new IllegalStateException();
            }
            OIdentifiable value = localCursor.next();
            return new Map.Entry() {

              @Override
              public Object getKey() {
                return item;
              }

              @Override
              public Object getValue() {

                return value;
              }

              @Override
              public Object setValue(Object value) {
                return null;
              }
            };
          }
        });
      }
      customIterator.reset();
    } else {
      cursor = createCursor(equals, definition, rightValue, ctx);
    }
    fetchNextEntry();
  }

  /**
   * it's not key = [...] but a real condition on field names, already ordered (field names will be ignored)
   */
  private void processAndBlock() {
    OCollection fromKey = indexKeyFrom((OAndBlock) condition, additionalRangeCondition);
    OCollection toKey = indexKeyTo((OAndBlock) condition, additionalRangeCondition);
    boolean fromKeyIncluded = indexKeyFromIncluded((OAndBlock) condition, additionalRangeCondition);
    boolean toKeyIncluded = indexKeyToIncluded((OAndBlock) condition, additionalRangeCondition);
    init(fromKey, fromKeyIncluded, toKey, toKeyIncluded);
  }

  private void processFlatIteration() {
    cursor = isOrderAsc() ? index.cursor() : index.descCursor();

    fetchNullKeys();
    if (cursor != null) {
      fetchNextEntry();
    }
  }

  private void fetchNullKeys() {
    Object nullIter = index.get(null);
    if (nullIter instanceof OIdentifiable) {
      nullKeyIterator = Collections.singleton(nullIter).iterator();
    } else if (nullIter instanceof Iterable) {
      nullKeyIterator = ((Iterable) nullIter).iterator();
    } else if (nullIter instanceof Iterator) {
      nullKeyIterator = (Iterator) nullIter;
    } else {
      nullKeyIterator = Collections.emptyIterator();
    }
  }

  private void init(OCollection fromKey, boolean fromKeyIncluded, OCollection toKey, boolean toKeyIncluded) {
    Object secondValue = fromKey.execute((OResult) null, ctx);
    Object thirdValue = toKey.execute((OResult) null, ctx);
    OIndexDefinition indexDef = index.getDefinition();
    secondValue = convertToIndexDefinitionTypes(secondValue, indexDef.getTypes());
    thirdValue = convertToIndexDefinitionTypes(thirdValue, indexDef.getTypes());
    if (index.supportsOrderedIterations()) {
      cursor = index
          .iterateEntriesBetween(toBetweenIndexKey(indexDef, secondValue), fromKeyIncluded, toBetweenIndexKey(indexDef, thirdValue),
              toKeyIncluded, isOrderAsc());
    } else if (additionalRangeCondition == null && allEqualities((OAndBlock) condition)) {
      cursor = index.iterateEntries(toIndexKey(indexDef, secondValue), isOrderAsc());
    } else {
      throw new UnsupportedOperationException("Cannot evaluate " + this.condition + " on index " + index);
    }
    if (cursor != null) {
      fetchNextEntry();
    }
  }

  private Object convertToIndexDefinitionTypes(Object val, OType[] types) {
    if (val == null) {
      return null;
    }
    if (OMultiValue.isMultiValue(val)) {
      List<Object> result = new ArrayList<>();
      int i = 0;
      for (Object o : OMultiValue.getMultiValueIterable(val)) {
        result.add(OType.convert(o, types[i++].getDefaultJavaType()));
      }
      return result;
    }
    return OType.convert(val, types[0].getDefaultJavaType());
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
    if (cursor != null) {
      fetchNextEntry();
    }
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
    if (cursor != null) {
      fetchNextEntry();
    }
  }

  private Collection toIndexKey(OIndexDefinition definition, Object rightValue) {
    if (definition.getFields().size() == 1 && rightValue instanceof Collection) {
      rightValue = ((Collection) rightValue).iterator().next();
    }
    if (rightValue instanceof List) {
      rightValue = definition.createValue((List<?>) rightValue);
    } else if (!(rightValue instanceof OCompositeKey)) {
      rightValue = definition.createValue(rightValue);
    }
    if (!(rightValue instanceof Collection)) {
      rightValue = Collections.singleton(rightValue);
    }
    return (Collection) rightValue;
  }

  private Object toBetweenIndexKey(OIndexDefinition definition, Object rightValue) {
    if (definition.getFields().size() == 1 && rightValue instanceof Collection) {
      if (((Collection) rightValue).size() > 0) {
        rightValue = ((Collection) rightValue).iterator().next();
      } else {
        rightValue = null;
      }
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
    String result = OExecutionStepInternal.getIndent(depth, indent) + "+ FETCH FROM INDEX " + index.getName();
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    if (condition != null) {
      result += ("\n" + OExecutionStepInternal.getIndent(depth, indent) + "  " + condition + (additionalRangeCondition == null ?
          "" :
          " and " + additionalRangeCondition));
    }

    return result;
  }

  @Override
  public long getCost() {
    return cost;
  }

  @Override
  public OResult serialize() {
    OResultInternal result = OExecutionStepInternal.basicSerialize(this);
    result.setProperty("indexName", index.getName());
    if (condition != null) {
      result.setProperty("condition", condition.serialize());
    }
    if (additionalRangeCondition != null) {
      result.setProperty("additionalRangeCondition", additionalRangeCondition.serialize());
    }
    result.setProperty("orderAsc", orderAsc);
    return result;
  }

  @Override
  public void deserialize(OResult fromResult) {
    try {
      OExecutionStepInternal.basicDeserialize(fromResult, this);
      indexName = fromResult.getProperty("indexName");
      if (fromResult.getProperty("condition") != null) {
        condition = OBooleanExpression.deserializeFromOResult(fromResult.getProperty("condition"));
      }
      if (fromResult.getProperty("additionalRangeCondition") != null) {
        additionalRangeCondition = new OBinaryCondition(-1);
        additionalRangeCondition.deserialize(fromResult.getProperty("additionalRangeCondition"));
      }
      orderAsc = fromResult.getProperty("orderAsc");
    } catch (Exception e) {
      throw new OCommandExecutionException("");
    }
  }
}
