package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.sql.parser.OAndBlock;
import com.orientechnologies.orient.core.sql.parser.OBinaryCompareOperator;
import com.orientechnologies.orient.core.sql.parser.OBinaryCondition;
import com.orientechnologies.orient.core.sql.parser.OBooleanExpression;

/** Created by luigidellaquila on 26/07/16. */
public class IndexSearchDescriptor {
  private OIndex index;
  private OAndBlock keyCondition;
  private OBinaryCondition additionalRangeCondition;
  private OBooleanExpression remainingCondition;

  public IndexSearchDescriptor(
      OIndex idx,
      OAndBlock keyCondition,
      OBinaryCondition additional,
      OBooleanExpression remainingCondition) {
    this.index = idx;
    this.keyCondition = keyCondition;
    this.additionalRangeCondition = additional;
    this.remainingCondition = remainingCondition;
  }

  public IndexSearchDescriptor(OIndex idx, OAndBlock keyCondition) {
    this.index = idx;
    this.keyCondition = keyCondition;
    this.additionalRangeCondition = null;
    this.remainingCondition = null;
  }

  public int cost(OCommandContext ctx) {
    OQueryStats stats = OQueryStats.get((ODatabaseDocumentInternal) ctx.getDatabase());

    String indexName = getIndex().getName();
    int size = getKeyCondition().getSubBlocks().size();
    boolean range = false;
    OBooleanExpression lastOp =
        getKeyCondition().getSubBlocks().get(getKeyCondition().getSubBlocks().size() - 1);
    if (lastOp instanceof OBinaryCondition) {
      OBinaryCompareOperator op = ((OBinaryCondition) lastOp).getOperator();
      range = op.isRangeOperator();
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

  protected OIndex getIndex() {
    return index;
  }

  protected OAndBlock getKeyCondition() {
    return keyCondition;
  }

  protected OBinaryCondition getAdditionalRangeCondition() {
    return additionalRangeCondition;
  }

  protected OBooleanExpression getRemainingCondition() {
    return remainingCondition;
  }
}
