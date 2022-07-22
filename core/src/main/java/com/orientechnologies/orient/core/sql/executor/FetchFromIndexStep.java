package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OExecutionThreadLocal;
import com.orientechnologies.orient.core.db.OSharedContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.viewmanager.ViewManager;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OCommandInterruptedException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexDefinitionMultiValue;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.parser.OAndBlock;
import com.orientechnologies.orient.core.sql.parser.OBetweenCondition;
import com.orientechnologies.orient.core.sql.parser.OBinaryCompareOperator;
import com.orientechnologies.orient.core.sql.parser.OBinaryCondition;
import com.orientechnologies.orient.core.sql.parser.OBooleanExpression;
import com.orientechnologies.orient.core.sql.parser.OCollection;
import com.orientechnologies.orient.core.sql.parser.OContainsAnyCondition;
import com.orientechnologies.orient.core.sql.parser.OContainsKeyOperator;
import com.orientechnologies.orient.core.sql.parser.OContainsTextCondition;
import com.orientechnologies.orient.core.sql.parser.OContainsValueCondition;
import com.orientechnologies.orient.core.sql.parser.OContainsValueOperator;
import com.orientechnologies.orient.core.sql.parser.OEqualsCompareOperator;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OGeOperator;
import com.orientechnologies.orient.core.sql.parser.OGtOperator;
import com.orientechnologies.orient.core.sql.parser.OInCondition;
import com.orientechnologies.orient.core.sql.parser.OLeOperator;
import com.orientechnologies.orient.core.sql.parser.OLtOperator;
import com.orientechnologies.orient.core.sql.parser.OValueExpression;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Created by luigidellaquila on 23/07/16. */
public class FetchFromIndexStep extends AbstractExecutionStep {
  protected OIndexInternal index;
  protected OBooleanExpression condition;
  private OBinaryCondition additionalRangeCondition;

  private boolean orderAsc;

  protected String indexName;

  private long cost = 0;
  private long count = 0;

  private boolean inited = false;
  private Stream<ORawPair<Object, ORID>> stream;
  private Iterator<ORawPair<Object, ORID>> indexIterator;

  private final List<Stream<ORawPair<Object, ORID>>> nextStreams = new ArrayList<>();

  private OMultiCollectionIterator<Map.Entry<Object, OIdentifiable>> customIterator;
  private Iterator nullKeyIterator;
  private ORawPair<Object, ORID> nextEntry = null;

  private final Set<Stream<ORawPair<Object, ORID>>> acquiredStreams =
      Collections.newSetFromMap(new IdentityHashMap<>());
  private final Set<Stream<ORID>> acquiredRidStreams = new HashSet<>();

  public FetchFromIndexStep(
      OIndex index,
      OBooleanExpression condition,
      OBinaryCondition additionalRangeCondition,
      OCommandContext ctx,
      boolean profilingEnabled) {
    this(index, condition, additionalRangeCondition, true, ctx, profilingEnabled);
  }

  public FetchFromIndexStep(
      OIndex index,
      OBooleanExpression condition,
      OBinaryCondition additionalRangeCondition,
      boolean orderAsc,
      OCommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.index = index.getInternal();
    this.indexName = index.getName();
    this.condition = condition;
    this.additionalRangeCondition = additionalRangeCondition;
    this.orderAsc = orderAsc;

    OSharedContext sharedContext =
        ((ODatabaseDocumentInternal) ctx.getDatabase()).getSharedContext();
    ViewManager viewManager = sharedContext.getViewManager();
    viewManager.startUsingViewIndex(indexName);
  }

  private FetchFromIndexStep(
      String indexName,
      OBooleanExpression condition,
      OBinaryCondition additionalRangeCondition,
      boolean orderAsc,
      OCommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.indexName = indexName;
    this.condition = condition;
    this.additionalRangeCondition = additionalRangeCondition;
    this.orderAsc = orderAsc;

    OSharedContext sharedContext =
        ((ODatabaseDocumentInternal) ctx.getDatabase()).getSharedContext();
    ViewManager viewManager = sharedContext.getViewManager();
    viewManager.startUsingViewIndex(indexName);
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    init(ctx.getDatabase());
    return new OResultSet() {
      private int localCount = 0;

      @Override
      public boolean hasNext() {
        if (timedOut) {
          throw new OTimeoutException("Command execution timeout");
        }

        if (localCount >= nRecords) {
          return false;
        }
        if (nextEntry == null) {
          fetchNextEntry();
        }
        return nextEntry != null;
      }

      @Override
      public OResult next() {
        if (!hasNext()) {
          throw new IllegalStateException();
        }
        if (localCount % 100 == 0 && OExecutionThreadLocal.isInterruptCurrentOperation()) {
          throw new OCommandInterruptedException("The command has been interrupted");
        }
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {
          Object key = nextEntry.first;
          OIdentifiable value = nextEntry.second;

          nextEntry = null;

          localCount++;
          OResultInternal result = new OResultInternal();
          result.setProperty("key", convertKey(key));
          result.setProperty("rid", value);
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

  private static Object convertKey(Object key) {
    if (key instanceof OCompositeKey) {
      return new ArrayList<>(((OCompositeKey) key).getKeys());
    }
    return key;
  }

  private void fetchNextEntry() {
    nextEntry = null;
    if (stream != null) {
      while (!indexIterator.hasNext()) {
        if (!nextStreams.isEmpty()) {
          stream = nextStreams.remove(0);
          storeAcquiredStream(stream);
          cursorToIterator();
        } else {
          stream = null;
          indexIterator = null;
          break;
        }
      }
      if (indexIterator != null) {
        nextEntry = indexIterator.next();
      }
    }
    if (nextEntry == null && customIterator != null && customIterator.hasNext()) {
      final Map.Entry<Object, OIdentifiable> entry = customIterator.next();
      nextEntry = new ORawPair<>(entry.getKey(), entry.getValue().getIdentity());
    }
    if (nextEntry == null && nullKeyIterator != null && nullKeyIterator.hasNext()) {
      OIdentifiable nextValue = (OIdentifiable) nullKeyIterator.next();
      nextEntry = new ORawPair<>(null, nextValue.getIdentity());
    }
    if (nextEntry == null) {
      updateIndexStats();
    } else {
      count++;
    }
  }

  private void storeAcquiredStream(Stream<ORawPair<Object, ORID>> stream) {
    if (stream != null) {
      acquiredStreams.add(stream);
    }
  }

  private void updateIndexStats() {
    // stats
    OQueryStats stats = OQueryStats.get((ODatabaseDocumentInternal) ctx.getDatabase());
    if (index == null) {
      return; // this could happen, if not inited yet
    }
    String indexName = index.getName();
    boolean range = false;
    int size = 0;

    if (condition != null) {
      if (condition instanceof OBinaryCondition) {
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
    }
    stats.pushIndexStats(indexName, size, range, additionalRangeCondition != null, count);
  }

  private synchronized void init(ODatabase db) {
    if (inited) {
      return;
    }
    inited = true;
    init(condition, (ODatabaseDocumentInternal) db);
  }

  private void init(OBooleanExpression condition, ODatabaseDocumentInternal db) {
    long begin = profilingEnabled ? System.nanoTime() : 0;
    if (index == null) {
      index = db.getMetadata().getIndexManagerInternal().getIndex(db, indexName).getInternal();
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
        processInCondition();
      } else {
        // TODO process containsAny
        throw new OCommandExecutionException(
            "search for index for " + condition + " is not supported yet");
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
      throw new OCommandExecutionException(
          "search for index for " + condition + " is not supported yet");
    }
    Object rightValue = inCondition.evaluateRight((OResult) null, ctx);
    OEqualsCompareOperator equals = new OEqualsCompareOperator(-1);
    if (OMultiValue.isMultiValue(rightValue)) {
      customIterator = new OMultiCollectionIterator<>();
      for (Object item : OMultiValue.getMultiValueIterable(rightValue)) {
        if (item instanceof OResult) {
          if (((OResult) item).isElement()) {
            item = ((OResult) item).getElement().orElseThrow(IllegalStateException::new);
          } else if (((OResult) item).getPropertyNames().size() == 1) {
            item =
                ((OResult) item).getProperty(((OResult) item).getPropertyNames().iterator().next());
          }
        }

        Iterator<ORawPair<Object, ORID>> localCursor =
            createCursor(equals, definition, item, ctx).iterator();
        final Object itemRef = item;
        customIterator.add(
            new Iterator<Map.Entry>() {
              @Override
              public boolean hasNext() {
                return localCursor.hasNext();
              }

              @Override
              public Map.Entry next() {
                if (!localCursor.hasNext()) {
                  throw new IllegalStateException();
                }

                ORawPair<Object, ORID> value = localCursor.next();
                return new Map.Entry() {
                  @Override
                  public Object getKey() {
                    return itemRef;
                  }

                  @Override
                  public Object getValue() {
                    return value.second;
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
      stream = createCursor(equals, definition, rightValue, ctx);
      storeAcquiredStream(stream);
      cursorToIterator();
    }
    fetchNextEntry();
  }

  /**
   * it's not key = [...] but a real condition on field names, already ordered (field names will be
   * ignored)
   */
  private void processAndBlock() {
    OCollection fromKey = indexKeyFrom((OAndBlock) condition, additionalRangeCondition);
    OCollection toKey = indexKeyTo((OAndBlock) condition, additionalRangeCondition);
    boolean fromKeyIncluded = indexKeyFromIncluded((OAndBlock) condition, additionalRangeCondition);
    boolean toKeyIncluded = indexKeyToIncluded((OAndBlock) condition, additionalRangeCondition);
    init(fromKey, fromKeyIncluded, toKey, toKeyIncluded);
  }

  private void processFlatIteration() {
    stream = isOrderAsc() ? index.stream() : index.descStream();
    storeAcquiredStream(stream);
    cursorToIterator();

    fetchNullKeys();
    if (stream != null) {
      fetchNextEntry();
    }
  }

  private void fetchNullKeys() {
    if (index.getDefinition().isNullValuesIgnored()) {
      nullKeyIterator = Collections.emptyIterator();
      return;
    }

    final Stream<ORID> stream = index.getRids(null);
    acquiredRidStreams.add(stream);

    nullKeyIterator = stream.iterator();
  }

  private void init(
      OCollection fromKey, boolean fromKeyIncluded, OCollection toKey, boolean toKeyIncluded) {
    List<OCollection> secondValueCombinations = cartesianProduct(fromKey);
    List<OCollection> thirdValueCombinations = cartesianProduct(toKey);

    OIndexDefinition indexDef = index.getDefinition();

    for (int i = 0; i < secondValueCombinations.size(); i++) {

      Object secondValue = secondValueCombinations.get(i).execute((OResult) null, ctx);
      if (secondValue instanceof List
          && ((List) secondValue).size() == 1
          && indexDef.getFields().size() == 1
          && !(indexDef instanceof OIndexDefinitionMultiValue)) {
        secondValue = ((List) secondValue).get(0);
      }
      secondValue = unboxOResult(secondValue);
      // TODO unwind collections!
      Object thirdValue = thirdValueCombinations.get(i).execute((OResult) null, ctx);
      if (thirdValue instanceof List
          && ((List) thirdValue).size() == 1
          && indexDef.getFields().size() == 1
          && !(indexDef instanceof OIndexDefinitionMultiValue)) {
        thirdValue = ((List) thirdValue).get(0);
      }
      thirdValue = unboxOResult(thirdValue);

      try {
        secondValue = convertToIndexDefinitionTypes(secondValue, indexDef.getTypes());
        thirdValue = convertToIndexDefinitionTypes(thirdValue, indexDef.getTypes());
      } catch (Exception e) {
        // manage subquery that returns a single collection
        if (secondValue instanceof Collection && secondValue.equals(thirdValue)) {
          ((Collection) secondValue)
              .forEach(
                  item -> {
                    Object itemVal = convertToIndexDefinitionTypes(item, indexDef.getTypes());
                    if (index.supportsOrderedIterations()) {

                      Object from = toBetweenIndexKey(indexDef, itemVal);
                      Object to = toBetweenIndexKey(indexDef, itemVal);
                      if (from == null && to == null) {
                        // manage null value explicitly, as the index API does not seem to work
                        // correctly in this
                        // case
                        stream = getStreamForNullKey();
                        storeAcquiredStream(stream);
                      } else {
                        stream =
                            index.streamEntriesBetween(
                                from, fromKeyIncluded, to, toKeyIncluded, isOrderAsc());
                        storeAcquiredStream(stream);
                      }

                    } else if (additionalRangeCondition == null
                        && allEqualities((OAndBlock) condition)) {
                      stream = index.streamEntries(toIndexKey(indexDef, itemVal), isOrderAsc());
                      storeAcquiredStream(stream);
                    } else if (isFullTextIndex(index)) {
                      stream = index.streamEntries(toIndexKey(indexDef, itemVal), isOrderAsc());
                      storeAcquiredStream(stream);
                    } else {
                      throw new UnsupportedOperationException(
                          "Cannot evaluate " + this.condition + " on index " + index);
                    }
                    nextStreams.add(stream);
                  });
        }

        // some problems in key conversion, so the params do not match the key types
        continue;
      }
      if (index.supportsOrderedIterations()) {

        Object from = toBetweenIndexKey(indexDef, secondValue);
        Object to = toBetweenIndexKey(indexDef, thirdValue);
        if (from == null && to == null) {
          // manage null value explicitly, as the index API does not seem to work correctly in this
          // case
          stream = getStreamForNullKey();
          storeAcquiredStream(stream);
        } else {
          stream =
              index.streamEntriesBetween(from, fromKeyIncluded, to, toKeyIncluded, isOrderAsc());
          storeAcquiredStream(stream);
        }

      } else if (additionalRangeCondition == null && allEqualities((OAndBlock) condition)) {
        stream = index.streamEntries(toIndexKey(indexDef, secondValue), isOrderAsc());
        storeAcquiredStream(stream);
      } else if (isFullTextIndex(index)) {
        stream = index.streamEntries(toIndexKey(indexDef, secondValue), isOrderAsc());
        storeAcquiredStream(stream);
      } else {
        throw new UnsupportedOperationException(
            "Cannot evaluate " + this.condition + " on index " + index);
      }
      nextStreams.add(stream);
    }
    if (nextStreams.size() > 0) {
      stream = nextStreams.remove(0);
      storeAcquiredStream(stream);
      cursorToIterator();
      fetchNextEntry();
    }
  }

  private void cursorToIterator() {
    if (stream != null) {
      indexIterator = stream.iterator();
    } else {
      indexIterator = null;
    }
  }

  private static boolean isFullTextIndex(OIndex index) {
    return index.getType().equalsIgnoreCase("FULLTEXT")
        && !index.getAlgorithm().equalsIgnoreCase("LUCENE");
  }

  private Stream<ORawPair<Object, ORID>> getStreamForNullKey() {
    final Stream<ORID> stream = index.getRids(null);
    acquiredRidStreams.add(stream);

    return stream.map((rid) -> new ORawPair<>(null, rid));
  }

  /**
   * this is for subqueries, when a OResult is found
   *
   * <ul>
   *   <li>if it's a projection with a single column, the value is returned
   *   <li>if it's a document, the RID is returned
   * </ul>
   */
  private static Object unboxOResult(Object value) {
    if (value instanceof List) {
      try (Stream stream = ((List) value).stream()) {
        //noinspection unchecked
        return stream.map(FetchFromIndexStep::unboxOResult).collect(Collectors.toList());
      }
    }
    if (value instanceof OResult) {
      if (((OResult) value).isElement()) {
        return ((OResult) value).getIdentity().orElse(null);
      }
      Set<String> props = ((OResult) value).getPropertyNames();
      if (props.size() == 1) {
        return ((OResult) value).getProperty(props.iterator().next());
      }
    }
    return value;
  }

  private List<OCollection> cartesianProduct(OCollection key) {
    return cartesianProduct(new OCollection(-1), key); // TODO
  }

  private List<OCollection> cartesianProduct(OCollection head, OCollection key) {
    if (key.getExpressions().size() == 0) {
      return Collections.singletonList(head);
    }
    OExpression nextElementInKey = key.getExpressions().get(0);
    Object value = nextElementInKey.execute(new OResultInternal(), ctx);
    if (value instanceof Iterable && !(value instanceof OIdentifiable)) {
      List<OCollection> result = new ArrayList<>();
      for (Object elemInKey : (Collection) value) {
        OCollection newHead = new OCollection(-1);
        for (OExpression exp : head.getExpressions()) {
          newHead.add(exp.copy());
        }
        newHead.add(toExpression(elemInKey, ctx));
        OCollection tail = key.copy();
        tail.getExpressions().remove(0);
        result.addAll(cartesianProduct(newHead, tail));
      }
      return result;
    } else {
      OCollection newHead = new OCollection(-1);
      for (OExpression exp : head.getExpressions()) {
        newHead.add(exp.copy());
      }
      newHead.add(nextElementInKey);
      OCollection tail = key.copy();
      tail.getExpressions().remove(0);
      return cartesianProduct(newHead, tail);
    }
  }

  private static OExpression toExpression(Object value, OCommandContext ctx) {
    return new OValueExpression(value);
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
      if (condition instanceof OAndBlock) {

        for (int j = 0; j < ((OAndBlock) condition).getSubBlocks().size(); j++) {
          OBooleanExpression subExp = ((OAndBlock) condition).getSubBlocks().get(j);
          if (subExp instanceof OBinaryCondition) {
            if (((OBinaryCondition) subExp).getOperator() instanceof OContainsKeyOperator) {
              Map<Object, Object> newValue = new HashMap<>();
              newValue.put(result.get(j), "");
              result.set(j, newValue);
            } else if (((OBinaryCondition) subExp).getOperator()
                instanceof OContainsValueOperator) {
              Map<Object, Object> newValue = new HashMap<>();
              newValue.put("", result.get(j));
              result.set(j, newValue);
            }
          } else if (subExp instanceof OContainsValueCondition) {
            Map<Object, Object> newValue = new HashMap<>();
            newValue.put("", result.get(j));
            result.set(j, newValue);
          }
        }
      }
      return result;
    }
    return OType.convert(val, types[0].getDefaultJavaType());
  }

  private static boolean allEqualities(OAndBlock condition) {
    if (condition == null) {
      return false;
    }
    for (OBooleanExpression exp : condition.getSubBlocks()) {
      if (exp instanceof OBinaryCondition) {
        if (!(((OBinaryCondition) exp).getOperator() instanceof OEqualsCompareOperator)
            && !(((OBinaryCondition) exp).getOperator() instanceof OContainsKeyOperator)
            && !(((OBinaryCondition) exp).getOperator() instanceof OContainsValueOperator)) {
          return false;
        }
      } else if (!(exp instanceof OInCondition)) {
        return false;
      } // OK
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
    secondValue = unboxOResult(secondValue);
    Object thirdValue = third.execute((OResult) null, ctx);
    thirdValue = unboxOResult(thirdValue);
    stream =
        index.streamEntriesBetween(
            toBetweenIndexKey(definition, secondValue),
            true,
            toBetweenIndexKey(definition, thirdValue),
            true,
            isOrderAsc());
    storeAcquiredStream(stream);
    cursorToIterator();
    if (stream != null) {
      fetchNextEntry();
    }
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
    stream = createCursor(operator, definition, rightValue, ctx);
    storeAcquiredStream(stream);
    cursorToIterator();
    if (stream != null) {
      fetchNextEntry();
    }
  }

  private static Collection toIndexKey(OIndexDefinition definition, Object rightValue) {
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

  private static Object toBetweenIndexKey(OIndexDefinition definition, Object rightValue) {
    if (definition.getFields().size() == 1 && rightValue instanceof Collection) {
      if (((Collection) rightValue).size() > 0) {
        rightValue = ((Collection) rightValue).iterator().next();
      } else {
        rightValue = null;
      }
    }

    if (rightValue instanceof Collection) {
      rightValue = definition.createValue(((Collection) rightValue).toArray());
    } else {
      rightValue = definition.createValue(rightValue);
    }

    return rightValue;
  }

  private Stream<ORawPair<Object, ORID>> createCursor(
      OBinaryCompareOperator operator,
      OIndexDefinition definition,
      Object value,
      OCommandContext ctx) {
    boolean orderAsc = isOrderAsc();
    if (operator instanceof OEqualsCompareOperator
        || operator instanceof OContainsKeyOperator
        || operator instanceof OContainsValueOperator) {
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
      OExpression res = exp.resolveKeyFrom(additional);
      if (res != null) {
        result.add(res);
      }
    }
    return result;
  }

  private static OCollection indexKeyTo(OAndBlock keyCondition, OBinaryCondition additional) {
    OCollection result = new OCollection(-1);
    for (OBooleanExpression exp : keyCondition.getSubBlocks()) {
      OExpression res = exp.resolveKeyTo(additional);
      if (res != null) {
        result.add(res);
      }
    }
    return result;
  }

  private static boolean indexKeyFromIncluded(OAndBlock keyCondition, OBinaryCondition additional) {
    OBooleanExpression exp =
        keyCondition.getSubBlocks().get(keyCondition.getSubBlocks().size() - 1);
    OBinaryCompareOperator additionalOperator =
        Optional.ofNullable(additional).map(OBinaryCondition::getOperator).orElse(null);
    if (exp instanceof OBinaryCondition) {
      OBinaryCompareOperator operator = ((OBinaryCondition) exp).getOperator();
      if (isGreaterOperator(operator)) {
        return isIncludeOperator(operator);
      } else
        return additionalOperator == null
            || (isIncludeOperator(additionalOperator) && isGreaterOperator(additionalOperator));
    } else if (exp instanceof OInCondition || exp instanceof OContainsAnyCondition) {
      return additional == null
          || (isIncludeOperator(additionalOperator) && isGreaterOperator(additionalOperator));
    } else if (exp instanceof OContainsTextCondition) {
      return true;
    } else if (exp instanceof OContainsValueCondition) {
      OBinaryCompareOperator operator = ((OContainsValueCondition) exp).getOperator();
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
    OBinaryCompareOperator additionalOperator =
        Optional.ofNullable(additional).map(OBinaryCondition::getOperator).orElse(null);
    if (exp instanceof OBinaryCondition) {
      OBinaryCompareOperator operator = ((OBinaryCondition) exp).getOperator();
      if (isLessOperator(operator)) {
        return isIncludeOperator(operator);
      } else
        return additionalOperator == null
            || (isIncludeOperator(additionalOperator) && isLessOperator(additionalOperator));
    } else if (exp instanceof OInCondition || exp instanceof OContainsAnyCondition) {
      return additionalOperator == null
          || (isIncludeOperator(additionalOperator) && isLessOperator(additionalOperator));
    } else if (exp instanceof OContainsTextCondition) {
      return true;
    } else if (exp instanceof OContainsValueCondition) {
      OBinaryCompareOperator operator = ((OContainsValueCondition) exp).getOperator();
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
        OExecutionStepInternal.getIndent(depth, indent) + "+ FETCH FROM INDEX " + indexName;
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    if (condition != null) {
      String additional =
          Optional.ofNullable(additionalRangeCondition)
              .map(rangeCondition -> " and " + rangeCondition)
              .orElse("");
      result +=
          ("\n" + OExecutionStepInternal.getIndent(depth, indent) + "  " + condition + additional);
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
      throw OException.wrapException(new OCommandExecutionException(""), e);
    }
  }

  @Override
  public void reset() {
    index = null;
    condition = Optional.ofNullable(condition).map(OBooleanExpression::copy).orElse(null);
    additionalRangeCondition =
        Optional.ofNullable(additionalRangeCondition).map(OBinaryCondition::copy).orElse(null);

    cost = 0;
    count = 0;

    inited = false;
    stream = null;
    indexIterator = null;
    customIterator = null;
    nullKeyIterator = null;
    nextEntry = null;
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new FetchFromIndexStep(
        indexName,
        Optional.ofNullable(this.condition).map(OBooleanExpression::copy).orElse(null),
        Optional.ofNullable(this.additionalRangeCondition).map(OBinaryCondition::copy).orElse(null),
        this.orderAsc,
        ctx,
        this.profilingEnabled);
  }

  private void closeStreams() {
    for (final Stream<ORawPair<Object, ORID>> stream : acquiredStreams) {
      stream.close();
    }

    acquiredStreams.clear();

    for (final Stream<ORID> stream : acquiredRidStreams) {
      stream.close();
    }

    acquiredRidStreams.clear();
  }

  @Override
  public void close() {
    super.close();

    closeStreams();
    OSharedContext sharedContext =
        ((ODatabaseDocumentInternal) ctx.getDatabase()).getSharedContext();
    ViewManager viewManager = sharedContext.getViewManager();
    viewManager.endUsingViewIndex(indexName);
  }
}
