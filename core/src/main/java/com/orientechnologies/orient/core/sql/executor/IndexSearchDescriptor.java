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
  protected OIndex idx;
  protected OAndBlock keyCondition;
  protected OBinaryCondition additionalRangeCondition;
  protected OBooleanExpression remainingCondition;

  public IndexSearchDescriptor(
      OIndex idx,
      OAndBlock keyCondition,
      OBinaryCondition additional,
      OBooleanExpression remainingCondition) {
    this.idx = idx;
    this.keyCondition = keyCondition;
    this.additionalRangeCondition = additional;
    this.remainingCondition = remainingCondition;
  }

  public IndexSearchDescriptor() {}

  public int cost(OCommandContext ctx) {
    OQueryStats stats = OQueryStats.get((ODatabaseDocumentInternal) ctx.getDatabase());

    String indexName = idx.getName();
    int size = keyCondition.getSubBlocks().size();
    boolean range = false;
    OBooleanExpression lastOp =
        keyCondition.getSubBlocks().get(keyCondition.getSubBlocks().size() - 1);
    if (lastOp instanceof OBinaryCondition) {
      OBinaryCompareOperator op = ((OBinaryCondition) lastOp).getOperator();
      range = op.isRangeOperator();
    }

    long val =
        stats.getIndexStats(
            indexName, size, range, additionalRangeCondition != null, ctx.getDatabase());
    if (val == -1) {
      // TODO query the index!
    }
    if (val >= 0) {
      return val > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) val;
    }
    return Integer.MAX_VALUE;
  }
}
