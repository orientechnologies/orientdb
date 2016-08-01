package com.orientechnologies.orient.core.sql.executor;

import java.util.List;

/**
 * Created by luigidellaquila on 20/07/16.
 */
public interface OExecutionStep {

  String getName();

  String getType();

  String getTargetNode();

  String getDescription();

  List<OExecutionStep> getSubSteps();
}
