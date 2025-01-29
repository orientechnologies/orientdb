package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
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
import com.orientechnologies.orient.core.sql.parser.OEqualsCompareOperator;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.sql.parser.OInCondition;
import com.orientechnologies.orient.core.sql.parser.OValueExpression;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Created by luigidellaquila on 26/07/16. */
public class IndexSearchDescriptor {
  private OIndex index;
  private OBooleanExpression keyCondition;
  private OBinaryCondition additionalRangeCondition;
  private OBooleanExpression remainingCondition;

  public IndexSearchDescriptor(
      OIndex idx,
      OBooleanExpression keyCondition,
      OBinaryCondition additional,
      OBooleanExpression remainingCondition) {
    this.index = idx;
    this.keyCondition = keyCondition;
    this.additionalRangeCondition = additional;
    this.remainingCondition = remainingCondition;
  }

  public IndexSearchDescriptor(OIndex idx) {
    this.index = idx;
    this.keyCondition = null;
    this.additionalRangeCondition = null;
    this.remainingCondition = null;
  }

  public IndexSearchDescriptor(OIndex idx, OBooleanExpression keyCondition) {
    this.index = idx;
    this.keyCondition = keyCondition;
    this.additionalRangeCondition = null;
    this.remainingCondition = null;
  }

  public int cost(OCommandContext ctx) {
    OQueryStats stats = OQueryStats.get((ODatabaseDocumentInternal) ctx.getDatabase());

    String indexName = getIndex().getName();
    int size = getSubBlocks().size();
    boolean range = false;
    OBooleanExpression lastOp = getSubBlocks().get(getSubBlocks().size() - 1);
    if (lastOp instanceof OBinaryCondition) {
      OBinaryCompareOperator op = ((OBinaryCondition) lastOp).getOperator();
      range = op.isRange();
    }

    long val =
        stats.getIndexStats(
            indexName, size, range, getAdditionalRangeCondition() != null, ctx.getDatabase());
    if (val == -1) {
      // TODO query the index!
    }
    if (val >= 0) {
      return val > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) val;
    }
    return Integer.MAX_VALUE;
  }

  private List<OBooleanExpression> getSubBlocks() {
    if (keyCondition instanceof OAndBlock) {
      return ((OAndBlock) keyCondition).getSubBlocks();
    } else {
      return Collections.singletonList(keyCondition);
    }
  }

  public int blockCount() {
    return getSubBlocks().size();
  }

  protected OIndex getIndex() {
    return index;
  }

  protected OBooleanExpression getKeyCondition() {
    return keyCondition;
  }

  protected OBinaryCondition getAdditionalRangeCondition() {
    return additionalRangeCondition;
  }

  protected OBooleanExpression getRemainingCondition() {
    return remainingCondition;
  }

  /**
   * checks whether the condition has CONTAINSANY or similar expressions, that require multiple index evaluations
   *
   * @param keyCondition
   * @return
   */
  public boolean requiresMultipleIndexLookups() {
    for (OBooleanExpression oBooleanExpression : getSubBlocks()) {
      if (!(oBooleanExpression instanceof OBinaryCondition)) {
        return true;
      }
    }
    return false;
  }

  public boolean requiresDistinctStep() {
    return requiresMultipleIndexLookups() || duplicateResultsForRecord();
  }

  public boolean duplicateResultsForRecord() {
    if (getIndex().getDefinition() instanceof OCompositeIndexDefinition) {
      if (((OCompositeIndexDefinition) getIndex().getDefinition()).getMultiValueDefinition()
          != null) {
        return true;
      }
    }
    return false;
  }

  public boolean fullySorted(List<String> orderItems) {
    List<OBooleanExpression> conditions = getSubBlocks();
    OIndex idx = getIndex();

    if (!idx.supportsOrderedIterations()) return false;
    List<String> conditionItems = new ArrayList<>();

    for (int i = 0; i < conditions.size(); i++) {
      OBooleanExpression item = conditions.get(i);
      if (item instanceof OBinaryCondition) {
        if (((OBinaryCondition) item).getOperator() instanceof OEqualsCompareOperator) {
          conditionItems.add(((OBinaryCondition) item).getLeft().toString());
        } else if (i != conditions.size() - 1) {
          return false;
        }

      } else if (i != conditions.size() - 1) {
        return false;
      }
    }

    List<String> orderedFields = new ArrayList<>();
    boolean overlapping = false;
    for (String s : conditionItems) {
      if (orderItems.isEmpty()) {
        return true; // nothing to sort, the conditions completely overlap the ORDER BY
      }
      if (s.equals(orderItems.get(0))) {
        orderItems.remove(0);
        overlapping = true; // start overlapping
      } else if (overlapping) {
        return false; // overlapping, but next order item does not match...
      }
      orderedFields.add(s);
    }
    orderedFields.addAll(orderItems);

    final OIndexDefinition definition = idx.getDefinition();
    final List<String> fields = definition.getFields();
    if (fields.size() < orderedFields.size()) {
      return false;
    }

    for (int i = 0; i < orderedFields.size(); i++) {
      final String orderFieldName = orderedFields.get(i);
      final String indexFieldName = fields.get(i);
      if (!orderFieldName.equals(indexFieldName)) {
        return false;
      }
    }

    return true;
  }

  /**
   * returns true if the first argument is a prefix for the second argument, eg. if the first argument is [a] and the second
   * argument is [a, b]
   *
   * @param item
   * @param descriptors
   * @return
   */
  public boolean isPrefixOf(IndexSearchDescriptor other) {
    List<OBooleanExpression> left = getSubBlocks();
    List<OBooleanExpression> right = other.getSubBlocks();
    if (left.size() > right.size()) {
      return false;
    }
    for (int i = 0; i < left.size(); i++) {
      if (!left.get(i).equals(right.get(i))) {
        return false;
      }
    }
    return true;
  }

  public boolean isSameCondition(IndexSearchDescriptor desc) {
    if (blockCount() != desc.blockCount()) {
      return false;
    }
    List<OBooleanExpression> left = getSubBlocks();
    List<OBooleanExpression> right = desc.getSubBlocks();
    for (int i = 0; i < left.size(); i++) {
      if (!left.get(i).equals(right.get(i))) {
        return false;
      }
    }
    return true;
  }

  public List<OIndexStream> getStreams(OCommandContext ctx, boolean isOrderAsc) {
    OIndexInternal index = getIndex().getInternal();
    OBooleanExpression condition = getKeyCondition();

    if (index.getDefinition() == null) {
      return Collections.emptyList();
    }
    if (condition == null) {
      List<OIndexStream> acquiredStreams = new ArrayList<>();
      acquiredStreams.add(new OAllIndexStream(index, isOrderAsc));

      if (!index.getDefinition().isNullValuesIgnored()) {
        acquiredStreams.add(new ONullIndexStream(index));
      }
      return acquiredStreams;
    } else if (condition instanceof OBinaryCondition) {
      return ((OBinaryCondition) condition).createIndexStreams(index, isOrderAsc, ctx);
    } else if (condition instanceof OBetweenCondition) {
      return ((OBetweenCondition) condition).createIndexStreams(index, isOrderAsc, ctx);
    } else if (condition instanceof OAndBlock) {
      return processAndBlock(isOrderAsc, ctx);
    } else if (condition instanceof OInCondition) {
      return ((OInCondition) condition).createIndexStreams(index, isOrderAsc, ctx);
    } else {
      // TODO process containsAny
      throw new OCommandExecutionException(
          "search for index for " + condition + " is not supported yet");
    }
  }

  private static boolean isNullKey(OIndexDefinition definition, Object rightValue) {
    if (definition.getFields().size() == 1 && rightValue instanceof Collection) {
      if (((Collection) rightValue).size() > 0) {
        rightValue = ((Collection) rightValue).iterator().next();
      } else {
        return true;
      }
    }
    return rightValue == null;
  }

  public static Object unboxOResult(Object value) {
    if (value instanceof List) {
      try (Stream stream = ((List) value).stream()) {
        // noinspection unchecked
        return stream.map(IndexSearchDescriptor::unboxOResult).collect(Collectors.toList());
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

  private List<OIndexStream> processAndBlock(boolean isOrderAsc, OCommandContext ctx) {
    OIndexInternal index = getIndex().getInternal();
    OBooleanExpression condition = getKeyCondition();
    OBinaryCondition additionalRangeCondition = getAdditionalRangeCondition();

    return multipleRange(index, condition, isOrderAsc, additionalRangeCondition, ctx);
  }

  private static List<OIndexStream> multipleRange(
      OIndexInternal index,
      OBooleanExpression condition,
      boolean isOrderAsc,
      OBinaryCondition additionalRangeCondition,
      OCommandContext ctx) {
    OCollection fromKey = ((OAndBlock) condition).indexKeyFrom(additionalRangeCondition);
    OCollection toKey = ((OAndBlock) condition).indexKeyTo(additionalRangeCondition);

    List<OIndexStream> acquiredStreams = new ArrayList<>();
    List<OCollection> secondValueCombinations = cartesianProduct(fromKey, ctx, isOrderAsc);
    List<OCollection> thirdValueCombinations = cartesianProduct(toKey, ctx, isOrderAsc);

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

      if (OMultiValue.isMultiValue(secondValue)
          && OMultiValue.getSize(secondValue) > indexDef.getTypes().length
          && OMultiValue.isMultiValue(thirdValue)
          && OMultiValue.getSize(thirdValue) > indexDef.getTypes().length) {
        if (secondValue instanceof Collection && secondValue.equals(thirdValue)) {
          ((Collection) secondValue)
              .forEach(
                  item -> {
                    Object itemVal =
                        convertToIndexDefinitionTypes(condition, item, indexDef.getTypes());
                    rangeIndexOps(
                        index,
                        (OAndBlock) condition,
                        isOrderAsc,
                        additionalRangeCondition,
                        acquiredStreams,
                        indexDef,
                        itemVal,
                        itemVal);
                  });
        }

        // some problems in key conversion, so the params do not match the key types
        continue;
      } else {
        secondValue = convertToIndexDefinitionTypes(condition, secondValue, indexDef.getTypes());
        thirdValue = convertToIndexDefinitionTypes(condition, thirdValue, indexDef.getTypes());
      }

      rangeIndexOps(
          index,
          (OAndBlock) condition,
          isOrderAsc,
          additionalRangeCondition,
          acquiredStreams,
          indexDef,
          secondValue,
          thirdValue);
    }
    return acquiredStreams;
  }

  protected static void rangeIndexOps(
      OIndexInternal index,
      OAndBlock condition,
      boolean isOrderAsc,
      OBinaryCondition additionalRangeCondition,
      List<OIndexStream> acquiredStreams,
      OIndexDefinition indexDef,
      Object fromVal,
      Object toVal) {

    if (index.supportsOrderedIterations()) {

      if (isNullKey(indexDef, fromVal) && isNullKey(indexDef, toVal)) {
        // manage null value explicitly, as the index API does not seem to work
        // correctly in this
        // case
        if (!index.getDefinition().isNullValuesIgnored()) {
          acquiredStreams.add(new ONullIndexStream(index));
        }
      } else {
        boolean fromKeyIncluded = condition.indexKeyFromIncluded(additionalRangeCondition);
        boolean toKeyIncluded = condition.indexKeyToIncluded(additionalRangeCondition);
        acquiredStreams.add(
            new OBetweenIndexStream(
                index, fromVal, fromKeyIncluded, toVal, toKeyIncluded, isOrderAsc));
      }

    } else if (additionalRangeCondition == null && condition != null && condition.allEqualities()) {
      acquiredStreams.add(new OExactIndexStream(index, fromVal, isOrderAsc));
    } else if (isFullTextIndex(index)) {
      acquiredStreams.add(new OExactIndexStream(index, fromVal, isOrderAsc));
    } else if (condition != null && condition.allNullCheck()) {
      if (!index.getDefinition().isNullValuesIgnored()) {
        acquiredStreams.add(new ONullIndexStream(index));
      }
    } else {
      throw new UnsupportedOperationException(
          "Cannot evaluate " + condition + " on index " + index);
    }
  }

  private static boolean isFullTextIndex(OIndex index) {
    return index.getType().equalsIgnoreCase("FULLTEXT")
        && !index.getAlgorithm().equalsIgnoreCase("LUCENE");
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
        ((OAndBlock) condition).matchTypesToCondition(result);
      }
      return result;
    }
    return OType.convert(val, types[0].getDefaultJavaType());
  }

  private static List<OCollection> cartesianProduct(
      OCollection key, OCommandContext ctx, boolean isOrderAsc) {
    return cartesianProduct(new OCollection(-1), key, ctx, isOrderAsc); // TODO
  }

  private static List<OCollection> cartesianProduct(
      OCollection head, OCollection key, OCommandContext ctx, boolean isOrderAsc) {
    if (key.getExpressions().size() == 0) {
      return Collections.singletonList(head);
    }
    OExpression nextElementInKey = key.getExpressions().get(0);
    Object value = nextElementInKey.execute(new OResultInternal(), ctx);
    if (value instanceof Iterable && !(value instanceof OIdentifiable)) {
      SortedSet<Object> ss;
      if (isOrderAsc) {
        ss = new TreeSet<>();
      } else {
        ss = new TreeSet<>((Comparator<Object>) Collections.reverseOrder());
      }
      for (Object elemInKey : (Collection) value) {
        ss.add(elemInKey);
      }
      List<OCollection> result = new ArrayList<>();
      for (Object elemInKey : ss) {
        OCollection newHead = new OCollection(-1);
        for (OExpression exp : head.getExpressions()) {
          newHead.add(exp.copy());
        }
        newHead.add(toExpression(elemInKey, ctx));
        OCollection tail = key.copy();
        tail.getExpressions().remove(0);
        result.addAll(cartesianProduct(newHead, tail, ctx, isOrderAsc));
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
      return cartesianProduct(newHead, tail, ctx, isOrderAsc);
    }
  }

  private static OExpression toExpression(Object value, OCommandContext ctx) {
    return new OValueExpression(value);
  }
}
