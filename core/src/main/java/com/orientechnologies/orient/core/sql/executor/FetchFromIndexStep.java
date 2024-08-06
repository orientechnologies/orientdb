package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OExecutionThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OCommandInterruptedException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexDefinitionMultiValue;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStreamProducer;
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
  protected IndexSearchDescriptor desc;

  private boolean orderAsc;

  private long count = 0;

  public FetchFromIndexStep(
      IndexSearchDescriptor desc, boolean orderAsc, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.desc = desc;
    this.orderAsc = orderAsc;

    ODatabaseDocumentInternal database = (ODatabaseDocumentInternal) ctx.getDatabase();
    database.queryStartUsingViewIndex(desc.getIndex().getName());
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.start(ctx).close(ctx));
    Set<Stream<ORawPair<Object, ORID>>> streams = init(desc, isOrderAsc(), ctx);

    OExecutionStreamProducer res =
        new OExecutionStreamProducer() {
          private final Iterator<Stream<ORawPair<Object, ORID>>> iter = streams.iterator();

          @Override
          public OExecutionStream next(OCommandContext ctx) {
            Stream<ORawPair<Object, ORID>> s = iter.next();
            return OExecutionStream.resultIterator(
                s.map((nextEntry) -> readResult(ctx, nextEntry)).iterator());
          }

          @Override
          public boolean hasNext(OCommandContext ctx) {
            return iter.hasNext();
          }

          @Override
          public void close(OCommandContext ctx) {
            while (iter.hasNext()) {
              iter.next().close();
            }
          }
        };
    return OExecutionStream.multipleStreams(res).onClose(this::close);
  }

  private void close(OCommandContext context) {
    updateIndexStats();
  }

  private OResult readResult(OCommandContext ctx, ORawPair<Object, ORID> nextEntry) {
    if (OExecutionThreadLocal.isInterruptCurrentOperation()) {
      throw new OCommandInterruptedException("The command has been interrupted");
    }
    count++;
    Object key = nextEntry.first;
    OIdentifiable value = nextEntry.second;

    nextEntry = null;

    OResultInternal result = new OResultInternal();
    result.setProperty("key", convertKey(key));
    result.setProperty("rid", value);
    ctx.setVariable("$current", result);
    return result;
  }

  private static Object convertKey(Object key) {
    if (key instanceof OCompositeKey) {
      return new ArrayList<>(((OCompositeKey) key).getKeys());
    }
    return key;
  }

  private void updateIndexStats() {
    // stats
    OQueryStats stats = OQueryStats.get((ODatabaseDocumentInternal) ctx.getDatabase());
    OIndex index = desc.getIndex();
    OBooleanExpression condition = desc.getKeyCondition();
    OBinaryCondition additionalRangeCondition = desc.getAdditionalRangeCondition();
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

  private static Set<Stream<ORawPair<Object, ORID>>> init(
      IndexSearchDescriptor desc, boolean isOrderAsc, OCommandContext ctx) {

    OIndexInternal index = desc.getIndex().getInternal();
    OBooleanExpression condition = desc.getKeyCondition();
    OBinaryCondition additionalRangeCondition = desc.getAdditionalRangeCondition();

    if (index.getDefinition() == null) {
      return Collections.emptySet();
    }
    if (condition == null) {
      return processFlatIteration(index, condition, isOrderAsc);
    } else if (condition instanceof OBinaryCondition) {
      return processBinaryCondition(index, condition, isOrderAsc, ctx);
    } else if (condition instanceof OBetweenCondition) {
      return processBetweenCondition(index, condition, isOrderAsc, ctx);
    } else if (condition instanceof OAndBlock) {
      return processAndBlock(index, condition, additionalRangeCondition, isOrderAsc, ctx);
    } else if (condition instanceof OInCondition) {
      return processInCondition(index, condition, ctx, isOrderAsc);
    } else {
      // TODO process containsAny
      throw new OCommandExecutionException(
          "search for index for " + condition + " is not supported yet");
    }
  }

  private static Set<Stream<ORawPair<Object, ORID>>> processInCondition(
      OIndexInternal index, OBooleanExpression condition, OCommandContext ctx, boolean orderAsc) {
    Set<Stream<ORawPair<Object, ORID>>> acquiredStreams =
        Collections.newSetFromMap(new IdentityHashMap<>());
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
      for (Object item : OMultiValue.getMultiValueIterable(rightValue)) {
        if (item instanceof OResult) {
          if (((OResult) item).isElement()) {
            item = ((OResult) item).getElement().orElseThrow(IllegalStateException::new);
          } else if (((OResult) item).getPropertyNames().size() == 1) {
            item =
                ((OResult) item).getProperty(((OResult) item).getPropertyNames().iterator().next());
          }
        }

        Stream<ORawPair<Object, ORID>> localCursor =
            createCursor(index, equals, definition, item, ctx, orderAsc, condition);

        acquiredStreams.add(localCursor);
      }
    } else {
      Stream<ORawPair<Object, ORID>> stream =
          createCursor(index, equals, definition, rightValue, ctx, orderAsc, condition);
      acquiredStreams.add(stream);
    }
    return acquiredStreams;
  }

  /**
   * it's not key = [...] but a real condition on field names, already ordered (field names will be
   * ignored)
   *
   * @param index
   * @param condition
   * @param acquiredStreams TODO
   */
  private static Set<Stream<ORawPair<Object, ORID>>> processAndBlock(
      OIndexInternal index,
      OBooleanExpression condition,
      OBinaryCondition additionalRangeCondition,
      boolean isOrderAsc,
      OCommandContext ctx) {
    OCollection fromKey = indexKeyFrom((OAndBlock) condition, additionalRangeCondition);
    OCollection toKey = indexKeyTo((OAndBlock) condition, additionalRangeCondition);
    boolean fromKeyIncluded = indexKeyFromIncluded((OAndBlock) condition, additionalRangeCondition);
    boolean toKeyIncluded = indexKeyToIncluded((OAndBlock) condition, additionalRangeCondition);
    return multipleRange(
        index,
        fromKey,
        fromKeyIncluded,
        toKey,
        toKeyIncluded,
        condition,
        isOrderAsc,
        additionalRangeCondition,
        ctx);
  }

  private static Set<Stream<ORawPair<Object, ORID>>> processFlatIteration(
      OIndexInternal index, OBooleanExpression condition, boolean isOrderAsc) {
    Set<Stream<ORawPair<Object, ORID>>> acquiredStreams =
        Collections.newSetFromMap(new IdentityHashMap<>());
    Stream<ORawPair<Object, ORID>> stream = isOrderAsc ? index.stream() : index.descStream();
    acquiredStreams.add(stream);

    stream = fetchNullKeys(index);
    if (stream != null) {
      acquiredStreams.add(stream);
    }
    return acquiredStreams;
  }

  private static Stream<ORawPair<Object, ORID>> fetchNullKeys(OIndexInternal index) {
    if (index.getDefinition().isNullValuesIgnored()) {
      return null;
    }

    return getStreamForNullKey(index);
  }

  private static Set<Stream<ORawPair<Object, ORID>>> multipleRange(
      OIndexInternal index,
      OCollection fromKey,
      boolean fromKeyIncluded,
      OCollection toKey,
      boolean toKeyIncluded,
      OBooleanExpression condition,
      boolean isOrderAsc,
      OBinaryCondition additionalRangeCondition,
      OCommandContext ctx) {
    Set<Stream<ORawPair<Object, ORID>>> acquiredStreams =
        Collections.newSetFromMap(new IdentityHashMap<>());
    List<OCollection> secondValueCombinations = cartesianProduct(fromKey, ctx);
    List<OCollection> thirdValueCombinations = cartesianProduct(toKey, ctx);

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
        secondValue = convertToIndexDefinitionTypes(condition, secondValue, indexDef.getTypes());
        thirdValue = convertToIndexDefinitionTypes(condition, thirdValue, indexDef.getTypes());
      } catch (Exception e) {
        // manage subquery that returns a single collection
        if (secondValue instanceof Collection && secondValue.equals(thirdValue)) {
          ((Collection) secondValue)
              .forEach(
                  item -> {
                    Stream<ORawPair<Object, ORID>> stream;
                    Object itemVal =
                        convertToIndexDefinitionTypes(condition, item, indexDef.getTypes());
                    if (index.supportsOrderedIterations()) {

                      Object from = toBetweenIndexKey(indexDef, itemVal);
                      Object to = toBetweenIndexKey(indexDef, itemVal);
                      if (from == null && to == null) {
                        // manage null value explicitly, as the index API does not seem to work
                        // correctly in this
                        // case
                        stream = getStreamForNullKey(index);
                        acquiredStreams.add(stream);
                      } else {
                        stream =
                            index.streamEntriesBetween(
                                from, fromKeyIncluded, to, toKeyIncluded, isOrderAsc);
                        acquiredStreams.add(stream);
                      }

                    } else if (additionalRangeCondition == null
                        && allEqualities((OAndBlock) condition)) {
                      stream = index.streamEntries(toIndexKey(indexDef, itemVal), isOrderAsc);
                      acquiredStreams.add(stream);
                    } else if (isFullTextIndex(index)) {
                      stream = index.streamEntries(toIndexKey(indexDef, itemVal), isOrderAsc);
                      acquiredStreams.add(stream);
                    } else {
                      throw new UnsupportedOperationException(
                          "Cannot evaluate " + condition + " on index " + index);
                    }
                  });
        }

        // some problems in key conversion, so the params do not match the key types
        continue;
      }
      Stream<ORawPair<Object, ORID>> stream;
      if (index.supportsOrderedIterations()) {

        Object from = toBetweenIndexKey(indexDef, secondValue);
        Object to = toBetweenIndexKey(indexDef, thirdValue);

        if (from == null && to == null) {
          // manage null value explicitly, as the index API does not seem to work correctly in this
          // case
          stream = getStreamForNullKey(index);
          acquiredStreams.add(stream);
        } else {
          stream = index.streamEntriesBetween(from, fromKeyIncluded, to, toKeyIncluded, isOrderAsc);
          acquiredStreams.add(stream);
        }

      } else if (additionalRangeCondition == null && allEqualities((OAndBlock) condition)) {
        stream = index.streamEntries(toIndexKey(indexDef, secondValue), isOrderAsc);
        acquiredStreams.add(stream);
      } else if (isFullTextIndex(index)) {
        stream = index.streamEntries(toIndexKey(indexDef, secondValue), isOrderAsc);
        acquiredStreams.add(stream);
      } else {
        throw new UnsupportedOperationException(
            "Cannot evaluate " + condition + " on index " + index);
      }
    }
    return acquiredStreams;
  }

  private static boolean isFullTextIndex(OIndex index) {
    return index.getType().equalsIgnoreCase("FULLTEXT")
        && !index.getAlgorithm().equalsIgnoreCase("LUCENE");
  }

  private static Stream<ORawPair<Object, ORID>> getStreamForNullKey(OIndexInternal index) {
    final Stream<ORID> stream = index.getRids(null);
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

  private static List<OCollection> cartesianProduct(OCollection key, OCommandContext ctx) {
    return cartesianProduct(new OCollection(-1), key, ctx); // TODO
  }

  private static List<OCollection> cartesianProduct(
      OCollection head, OCollection key, OCommandContext ctx) {
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
        result.addAll(cartesianProduct(newHead, tail, ctx));
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
      return cartesianProduct(newHead, tail, ctx);
    }
  }

  private static OExpression toExpression(Object value, OCommandContext ctx) {
    return new OValueExpression(value);
  }

  private static Object convertToIndexDefinitionTypes(
      OBooleanExpression condition, Object val, OType[] types) {
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

  private static Set<Stream<ORawPair<Object, ORID>>> processBetweenCondition(
      OIndexInternal index, OBooleanExpression condition, boolean isOrderAsc, OCommandContext ctx) {
    Set<Stream<ORawPair<Object, ORID>>> acquiredStreams =
        Collections.newSetFromMap(new IdentityHashMap<>());
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
    Stream<ORawPair<Object, ORID>> stream =
        index.streamEntriesBetween(
            toBetweenIndexKey(definition, secondValue),
            true,
            toBetweenIndexKey(definition, thirdValue),
            true,
            isOrderAsc);
    acquiredStreams.add(stream);
    return acquiredStreams;
  }

  private static Set<Stream<ORawPair<Object, ORID>>> processBinaryCondition(
      OIndexInternal index, OBooleanExpression condition, boolean isOrderAsc, OCommandContext ctx) {
    Set<Stream<ORawPair<Object, ORID>>> acquiredStreams =
        Collections.newSetFromMap(new IdentityHashMap<>());
    OIndexDefinition definition = index.getDefinition();
    OBinaryCompareOperator operator = ((OBinaryCondition) condition).getOperator();
    OExpression left = ((OBinaryCondition) condition).getLeft();
    if (!left.toString().equalsIgnoreCase("key")) {
      throw new OCommandExecutionException(
          "search for index for " + condition + " is not supported yet");
    }
    Object rightValue = ((OBinaryCondition) condition).getRight().execute((OResult) null, ctx);
    Stream<ORawPair<Object, ORID>> stream =
        createCursor(index, operator, definition, rightValue, ctx, isOrderAsc, condition);
    acquiredStreams.add(stream);
    return acquiredStreams;
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

  private static Stream<ORawPair<Object, ORID>> createCursor(
      OIndexInternal index,
      OBinaryCompareOperator operator,
      OIndexDefinition definition,
      Object value,
      OCommandContext ctx,
      boolean orderAsc,
      OBooleanExpression condition) {
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
        OExecutionStepInternal.getIndent(depth, indent)
            + "+ FETCH FROM INDEX "
            + desc.getIndex().getName();
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    if (desc.getKeyCondition() != null) {
      String additional =
          Optional.ofNullable(desc.getAdditionalRangeCondition())
              .map(rangeCondition -> " and " + rangeCondition)
              .orElse("");
      result +=
          ("\n"
              + OExecutionStepInternal.getIndent(depth, indent)
              + "  "
              + desc.getKeyCondition()
              + additional);
    }

    return result;
  }

  @Override
  public void serializeToResult(OResultInternal result) {
    result.setProperty("index", desc.getIndex().getName());
    if (desc.getKeyCondition() != null) {
      result.setProperty("key", desc.getKeyCondition().toString());
      if (desc.getAdditionalRangeCondition() != null) {
        result.setProperty("toKey", desc.getAdditionalRangeCondition().toString());
      }
    }
  }

  @Override
  public OResult serialize() {
    OResultInternal result = OExecutionStepInternal.basicSerialize(this);
    result.setProperty("indexName", desc.getIndex().getName());
    if (desc.getKeyCondition() != null) {
      result.setProperty("condition", desc.getKeyCondition().serialize());
    }
    if (desc.getAdditionalRangeCondition() != null) {
      result.setProperty(
          "additionalRangeCondition", desc.getAdditionalRangeCondition().serialize());
    }
    result.setProperty("orderAsc", orderAsc);
    return result;
  }

  @Override
  public void deserialize(OResult fromResult) {
    try {
      OExecutionStepInternal.basicDeserialize(fromResult, this);
      String indexName = fromResult.getProperty("indexName");
      OBooleanExpression condition = null;
      if (fromResult.getProperty("condition") != null) {
        condition = OBooleanExpression.deserializeFromOResult(fromResult.getProperty("condition"));
      }
      OBinaryCondition additionalRangeCondition = null;
      if (fromResult.getProperty("additionalRangeCondition") != null) {
        additionalRangeCondition = new OBinaryCondition(-1);
        additionalRangeCondition.deserialize(fromResult.getProperty("additionalRangeCondition"));
      }
      ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().get();
      OIndex index = db.getMetadata().getIndexManager().getIndex(indexName);
      desc = new IndexSearchDescriptor(index, condition, additionalRangeCondition, null);
      orderAsc = fromResult.getProperty("orderAsc");
    } catch (Exception e) {
      throw OException.wrapException(new OCommandExecutionException(""), e);
    }
  }

  @Override
  public void reset() {
    desc = null;
    count = 0;
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new FetchFromIndexStep(desc, this.orderAsc, ctx, this.profilingEnabled);
  }

  @Override
  public void close() {
    super.close();
  }

  public String getIndexName() {
    return desc.getIndex().getName();
  }
}
