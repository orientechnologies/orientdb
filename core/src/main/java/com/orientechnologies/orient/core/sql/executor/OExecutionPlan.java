package com.orientechnologies.orient.core.sql.executor;

import java.io.Serializable;
import java.util.List;

/** Created by luigidellaquila on 06/07/16. */
public interface OExecutionPlan extends Serializable {

  List<OExecutionStep> getSteps();

  String prettyPrint(int depth, int indent);

  OResult toResult();
}
