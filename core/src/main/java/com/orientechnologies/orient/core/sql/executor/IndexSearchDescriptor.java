package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.sql.parser.OAndBlock;
import com.orientechnologies.orient.core.sql.parser.OBinaryCompareOperator;
import com.orientechnologies.orient.core.sql.parser.OBinaryCondition;
import com.orientechnologies.orient.core.sql.parser.OBooleanExpression;
import com.orientechnologies.orient.core.sql.parser.OEqualsCompareOperator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
   * checks whether the condition has CONTAINSANY or similar expressions, that require multiple
   * index evaluations
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
   * returns true if the first argument is a prefix for the second argument, eg. if the first
   * argument is [a] and the second argument is [a, b]
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
}
