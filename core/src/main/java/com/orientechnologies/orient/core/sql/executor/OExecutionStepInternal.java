package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by luigidellaquila on 06/07/16.
 */
public interface OExecutionStepInternal extends OExecutionStep, OExecutionCallback {

  OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException;

  void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException;

  void sendTimeout();

  void setPrevious(OExecutionStepInternal step);

  void setNext(OExecutionStepInternal step);

  void close();

  static String getIndent(int depth, int indent) {
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < depth; i++) {
      for (int j = 0; j < indent; j++) {
        result.append(" ");
      }
    }
    return result.toString();
  }

  default String prettyPrint(int depth, int indent) {
    String spaces = getIndent(depth, indent);
    return spaces + getClass().getSimpleName();
  }

  default String getName() {
    return getClass().getSimpleName();
  }

  default String getType() {
    return getClass().getSimpleName();
  }

  default String getDescription() {
    return prettyPrint(0, 3);
  }

  default String getTargetNode() {
    return "<local>";
  }

  default List<OExecutionStep> getSubSteps() {
    return Collections.EMPTY_LIST;
  }

  default List<OExecutionPlan> getSubExecutionPlans() {
    return Collections.EMPTY_LIST;
  }

  default void reset() {
    //do nothing
  }

  default OResult serialize() {
    throw new UnsupportedOperationException();
  }

  default void deserialize(OResult fromResult) {
    throw new UnsupportedOperationException();
  }

  static OResultInternal basicSerialize(OExecutionStepInternal step) {
    OResultInternal result = new OResultInternal();
    result.setProperty(OInternalExecutionPlan.JAVA_TYPE, step.getClass().getName());
    if (step.getSubSteps() != null && step.getSubSteps().size() > 0) {
      List<OResult> serializedSubsteps = new ArrayList<>();
      for (OExecutionStep substep : step.getSubSteps()) {
        serializedSubsteps.add(((OExecutionStepInternal) substep).serialize());
      }
      result.setProperty("subSteps", serializedSubsteps);
    }

    if (step.getSubExecutionPlans() != null && step.getSubExecutionPlans().size() > 0) {
      List<OResult> serializedSubPlans = new ArrayList<>();
      for (OExecutionPlan substep : step.getSubExecutionPlans()) {
        serializedSubPlans.add(((OInternalExecutionPlan) substep).serialize());
      }
      result.setProperty("subExecutionPlans", serializedSubPlans);
    }
    return result;
  }

  static void basicDeserialize(OResult serialized, OExecutionStepInternal step)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    List<OResult> serializedSubsteps = serialized.getProperty("subSteps");
    if (serializedSubsteps != null) {
      for (OResult serializedSub : serializedSubsteps) {
        String className = serializedSub.getProperty(OInternalExecutionPlan.JAVA_TYPE);
        OExecutionStepInternal subStep = (OExecutionStepInternal) Class.forName(className).newInstance();
        subStep.deserialize(serializedSub);
        step.getSubSteps().add(subStep);
      }
    }

    List<OResult> serializedPlans= serialized.getProperty("subExecutionPlans");
    if (serializedSubsteps != null) {
      for (OResult serializedSub : serializedPlans) {
        String className = serializedSub.getProperty(OInternalExecutionPlan.JAVA_TYPE);
        OInternalExecutionPlan subStep = (OInternalExecutionPlan) Class.forName(className).newInstance();
        subStep.deserialize(serializedSub);
        step.getSubExecutionPlans().add(subStep);
      }
    }
  }
}
