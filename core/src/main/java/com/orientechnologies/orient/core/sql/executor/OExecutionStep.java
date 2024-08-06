package com.orientechnologies.orient.core.sql.executor;

import java.util.List;

/** Created by luigidellaquila on 20/07/16. */
public interface OExecutionStep {

  String getName();

  String getType();

  String getTargetNode();

  String getDescription();

  List<OExecutionStep> getSubSteps();

  /**
   * returns the absolute cost (in nanoseconds) of the execution of this step
   *
   * @return the absolute cost (in nanoseconds) of the execution of this step, -1 if not calculated
   */
  default long getCost() {
    return -1l;
  }

  OResult toResult();
}
