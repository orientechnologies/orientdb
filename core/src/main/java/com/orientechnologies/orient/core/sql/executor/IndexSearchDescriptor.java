package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.sql.parser.OAndBlock;
import com.orientechnologies.orient.core.sql.parser.OBooleanExpression;

/**
 * Created by luigidellaquila on 26/07/16.
 */
public class IndexSearchDescriptor {
  protected OIndex             idx;
  protected OAndBlock          keyCondition;
  protected OBooleanExpression remainingCondition;

  public IndexSearchDescriptor(OIndex idx, OAndBlock keyCondition, OBooleanExpression remainingCondition) {
    this.idx = idx;
    this.keyCondition = keyCondition;
    this.remainingCondition = remainingCondition;
  }

  public IndexSearchDescriptor() {

  }

  public int cost(OCommandContext ctx) {
    return Integer.MAX_VALUE;
  }
}
