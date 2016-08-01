package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.sql.parser.OAndBlock;
import com.orientechnologies.orient.core.sql.parser.OBinaryCondition;
import com.orientechnologies.orient.core.sql.parser.OBooleanExpression;

/**
 * Created by luigidellaquila on 26/07/16.
 */
public class IndexSearchDescriptor {
  protected OIndex             idx;
  protected OAndBlock          keyCondition;
  protected OBinaryCondition   additionalRangeCondition;
  protected OBooleanExpression remainingCondition;

  public IndexSearchDescriptor(OIndex idx, OAndBlock keyCondition, OBinaryCondition additional,
      OBooleanExpression remainingCondition) {
    this.idx = idx;
    this.keyCondition = keyCondition;
    this.additionalRangeCondition = additional;
    this.remainingCondition = remainingCondition;
  }

  public IndexSearchDescriptor() {

  }

  public int cost(OCommandContext ctx) {
    return Integer.MAX_VALUE;
  }
}
