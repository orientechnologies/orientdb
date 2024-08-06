package com.orientechnologies.orient.core.sql.executor;

import java.util.List;
import java.util.Set;

/** Created by luigidellaquila on 06/07/16. */
public interface OExecutionPlan {

  List<OExecutionStep> getSteps();

  String prettyPrint(int depth, int indent);

  OResult toResult();

  Set<String> getIndexes();
}
