package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.sql.parser.WhileStep;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OForEachExecutionPlan extends OUpdateExecutionPlan {
  public OForEachExecutionPlan() {
    super();
  }

  public boolean containsReturn() {
    for (OExecutionStep step : getSteps()) {
      if (step instanceof ForEachStep) {
        return ((ForEachStep) step).containsReturn();
      }
      if (step instanceof WhileStep) {
        return ((WhileStep) step).containsReturn();
      }
    }

    return false;
  }
}
