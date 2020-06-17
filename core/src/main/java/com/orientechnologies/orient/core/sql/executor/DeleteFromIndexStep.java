package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.sql.parser.OAndBlock;
import com.orientechnologies.orient.core.sql.parser.OBetweenCondition;
import com.orientechnologies.orient.core.sql.parser.OBinaryCompareOperator;
import com.orientechnologies.orient.core.sql.parser.OBinaryCondition;
import com.orientechnologies.orient.core.sql.parser.OBooleanExpression;
import com.orientechnologies.orient.core.sql.parser.OCollection;
import com.orientechnologies.orient.core.sql.parser.OEqualsCompareOperator;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OGeOperator;
import com.orientechnologies.orient.core.sql.parser.OGtOperator;
import com.orientechnologies.orient.core.sql.parser.OLeOperator;
import com.orientechnologies.orient.core.sql.parser.OLtOperator;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/** Created by luigidellaquila on 11/08/16. */
public class DeleteFromIndexStep extends AbstractExecutionStep {
  protected final OIndexInternal index;
  private final OBinaryCondition additional;
  private final OBooleanExpression ridCondition;
  private final boolean orderAsc;

  private ORawPair<Object, ORID> nextEntry = null;

  private final OBooleanExpression condition;

  private boolean inited = false;
  private Stream<ORawPair<Object, ORID>> stream;
  private Iterator<ORawPair<Object, ORID>> streamIterator;

  private long cost = 0;

  private final Set<Stream<ORawPair<Object, ORID>>> acquiredStreams =
      Collections.newSetFromMap(new IdentityHashMap<>());

  public DeleteFromIndexStep(
      OIndex index,
      OBooleanExpression condition,
      OBinaryCondition additionalRangeCondition,
      OBooleanExpression ridCondition,
      OCommandContext ctx,
      boolean profilingEnabled) {
    this(index, condition, additionalRangeCondition, ridCondition, true, ctx, profilingEnabled);
  }

  private DeleteFromIndexStep(
      OIndex index,
      OBooleanExpression condition,
      OBinaryCondition additionalRangeCondition,
      OBooleanExpression ridCondition,
      boolean orderAsc,
      OCommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.index = index.getInternal();
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
      private int localCount = 0;

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
          ORawPair<Object, ORID> entry = nextEntry;
          OResultInternal result = new OResultInternal();
          ORID value = entry.second;

          index.remove(entry.first, value);
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
        closeStreams();
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

  private void closeStreams() {
    for (Stream<ORawPair<Object, ORID>> stream : acquiredStreams) {
      stream.close();
    }
    acquiredStreams.clear();
  }

  @Override
  public void close() {
    super.close();

    closeStreams();
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

  private ORawPair<Object, ORID> loadNextEntry(OCommandContext commandContext) {
    while (streamIterator.hasNext()) {
      final ORawPair<Object, ORID> entry = streamIterator.next();
      if (ridCondition == null) {
        return entry;
      }
      OResultInternal res = new OResultInternal();
      res.setProperty("rid", entry.second);
      if (ridCondition.evaluate(res, commandContext)) {
        return entry;
      }
    }

    return null;
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
      throw new OCommandExecutionException(
          "search for index for " + condition + " is not supported yet");
    }
  }

  /**
   * it's not key = [...] but a real condition on field names, already ordered (field names will be
   * ignored)
   */
  private void processAndBlock() {
    OCollection fromKey = indexKeyFrom((OAndBlock) condition, additional);
    OCollection toKey = indexKeyTo((OAndBlock) condition, additional);
    boolean fromKeyIncluded = indexKeyFromIncluded((OAndBlock) condition, additional);
    boolean toKeyIncluded = indexKeyToIncluded((OAndBlock) condition, additional);
    init(fromKey, fromKeyIncluded, toKey, toKeyIncluded);
  }

  private void processFlatIteration() {
    stream = orderAsc ? index.stream() : index.descStream();
    storeAcquiredStream(stream);
    streamIterator = stream.iterator();
  }

  private void storeAcquiredStream(Stream<ORawPair<Object, ORID>> stream) {
    if (stream != null) {
      acquiredStreams.add(stream);
    }
  }

  private void init(
      OCollection fromKey, boolean fromKeyIncluded, OCollection toKey, boolean toKeyIncluded) {
    Object secondValue = fromKey.execute((OResult) null, ctx);
    Object thirdValue = toKey.execute((OResult) null, ctx);
    OIndexDefinition indexDef = index.getDefinition();
    if (index.supportsOrderedIterations()) {
      stream =
          index.streamEntriesBetween(
              toBetweenIndexKey(indexDef, secondValue),
              fromKeyIncluded,
              toBetweenIndexKey(indexDef, thirdValue),
              toKeyIncluded,
              orderAsc);
      storeAcquiredStream(stream);
    } else if (additional == null && allEqualities((OAndBlock) condition)) {
      stream = index.streamEntries(toIndexKey(indexDef, secondValue), orderAsc);
      storeAcquiredStream(stream);
    } else {
      throw new UnsupportedOperationException(
          "Cannot evaluate " + this.condition + " on index " + index);
    }

    streamIterator = stream.iterator();
  }

  private static boolean allEqualities(OAndBlock condition) {
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
      throw new OCommandExecutionException(
          "search for index for " + condition + " is not supported yet");
    }
    OExpression second = ((OBetweenCondition) condition).getSecond();
    OExpression third = ((OBetweenCondition) condition).getThird();

    Object secondValue = second.execute((OResult) null, ctx);
    Object thirdValue = third.execute((OResult) null, ctx);
    stream =
        index.streamEntriesBetween(
            toBetweenIndexKey(definition, secondValue),
            true,
            toBetweenIndexKey(definition, thirdValue),
            true,
            orderAsc);
    storeAcquiredStream(stream);
    streamIterator = stream.iterator();
  }

  private void processBinaryCondition() {
    OIndexDefinition definition = index.getDefinition();
    OBinaryCompareOperator operator = ((OBinaryCondition) condition).getOperator();
    OExpression left = ((OBinaryCondition) condition).getLeft();
    if (!left.toString().equalsIgnoreCase("key")) {
      throw new OCommandExecutionException(
          "search for index for " + condition + " is not supported yet");
    }
    Object rightValue = ((OBinaryCondition) condition).getRight().execute((OResult) null, ctx);
    stream = createStream(operator, definition, rightValue, ctx);
    storeAcquiredStream(stream);
    streamIterator = stream.iterator();
  }

  private static Collection toIndexKey(OIndexDefinition definition, Object rightValue) {
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

  private static Object toBetweenIndexKey(OIndexDefinition definition, Object rightValue) {
    if (definition.getFields().size() == 1 && rightValue instanceof Collection) {
      rightValue = ((Collection) rightValue).iterator().next();
    }
    rightValue = definition.createValue(rightValue);

    if (definition.getFields().size() > 1 && !(rightValue instanceof Collection)) {
      rightValue = Collections.singleton(rightValue);
    }
    return rightValue;
  }

  private Stream<ORawPair<Object, ORID>> createStream(
      OBinaryCompareOperator operator,
      OIndexDefinition definition,
      Object value,
      OCommandContext ctx) {
    boolean orderAsc = this.orderAsc;
    if (operator instanceof OEqualsCompareOperator) {
      return index.streamEntries(toIndexKey(definition, value), orderAsc);
    } else if (operator instanceof OGeOperator) {
      return index.streamEntriesMajor(value, true, orderAsc);
    } else if (operator instanceof OGtOperator) {
      return index.streamEntriesMajor(value, false, orderAsc);
    } else if (operator instanceof OLeOperator) {
      return index.streamEntriesMinor(value, true, orderAsc);
    } else if (operator instanceof OLtOperator) {
      return index.streamEntriesMinor(value, false, orderAsc);
    } else {
      throw new OCommandExecutionException(
          "search for index for " + condition + " is not supported yet");
    }
  }

  protected boolean isOrderAsc() {
    return orderAsc;
  }

  private static OCollection indexKeyFrom(OAndBlock keyCondition, OBinaryCondition additional) {
    OCollection result = new OCollection(-1);
    for (OBooleanExpression exp : keyCondition.getSubBlocks()) {
      if (exp instanceof OBinaryCondition) {
        OBinaryCondition binaryCond = ((OBinaryCondition) exp);
        OBinaryCompareOperator operator = binaryCond.getOperator();
        if ((operator instanceof OEqualsCompareOperator)
            || (operator instanceof OGtOperator)
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

  private static OCollection indexKeyTo(OAndBlock keyCondition, OBinaryCondition additional) {
    OCollection result = new OCollection(-1);
    for (OBooleanExpression exp : keyCondition.getSubBlocks()) {
      if (exp instanceof OBinaryCondition) {
        OBinaryCondition binaryCond = ((OBinaryCondition) exp);
        OBinaryCompareOperator operator = binaryCond.getOperator();
        if ((operator instanceof OEqualsCompareOperator)
            || (operator instanceof OLtOperator)
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

  private static boolean indexKeyFromIncluded(OAndBlock keyCondition, OBinaryCondition additional) {
    OBooleanExpression exp =
        keyCondition.getSubBlocks().get(keyCondition.getSubBlocks().size() - 1);
    if (exp instanceof OBinaryCondition) {
      OBinaryCompareOperator operator = ((OBinaryCondition) exp).getOperator();
      OBinaryCompareOperator additionalOperator =
          Optional.ofNullable(additional).map(OBinaryCondition::getOperator).orElse(null);
      if (isGreaterOperator(operator)) {
        return isIncludeOperator(operator);
      } else
        return additionalOperator == null
            || (isIncludeOperator(additionalOperator) && isGreaterOperator(additionalOperator));
    } else {
      throw new UnsupportedOperationException("Cannot execute index query with " + exp);
    }
  }

  private static boolean isGreaterOperator(OBinaryCompareOperator operator) {
    if (operator == null) {
      return false;
    }
    return operator instanceof OGeOperator || operator instanceof OGtOperator;
  }

  private static boolean isLessOperator(OBinaryCompareOperator operator) {
    if (operator == null) {
      return false;
    }
    return operator instanceof OLeOperator || operator instanceof OLtOperator;
  }

  private static boolean isIncludeOperator(OBinaryCompareOperator operator) {
    if (operator == null) {
      return false;
    }
    return operator instanceof OGeOperator || operator instanceof OLeOperator;
  }

  private static boolean indexKeyToIncluded(OAndBlock keyCondition, OBinaryCondition additional) {
    OBooleanExpression exp =
        keyCondition.getSubBlocks().get(keyCondition.getSubBlocks().size() - 1);
    if (exp instanceof OBinaryCondition) {
      OBinaryCompareOperator operator = ((OBinaryCondition) exp).getOperator();
      OBinaryCompareOperator additionalOperator =
          Optional.ofNullable(additional).map(OBinaryCondition::getOperator).orElse(null);
      if (isLessOperator(operator)) {
        return isIncludeOperator(operator);
      } else
        return additionalOperator == null
            || (isIncludeOperator(additionalOperator) && isLessOperator(additionalOperator));
    } else {
      throw new UnsupportedOperationException("Cannot execute index query with " + exp);
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String result =
        OExecutionStepInternal.getIndent(depth, indent) + "+ DELETE FROM INDEX " + index.getName();
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    String additional =
        Optional.ofNullable(this.additional)
            .map(oBinaryCondition -> " and " + oBinaryCondition)
            .orElse("");
    result +=
        (Optional.ofNullable(condition)
            .map(
                oBooleanExpression ->
                    ("\n"
                        + OExecutionStepInternal.getIndent(depth, indent)
                        + "  "
                        + oBooleanExpression
                        + additional))
            .orElse(""));
    return result;
  }

  @Override
  public long getCost() {
    return cost;
  }
}
