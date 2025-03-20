package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Execution Steps are the building blocks of a query execution plan
 *
 * <p>Typically an execution plan is made of a chain of steps. The execution is pull-based, meaning
 * that the result set that the client iterates is conceptually the one returned by <i>last</i> step
 * of the execution plan
 *
 * <p>At each `next()` invocation, the step typically fetches a record from the previous (upstream)
 * step, does its elaboration (eg. for a filtering step, it can discard the record and fetch another
 * one if it doesn't match the conditions) and returns the elaborated step
 *
 * <p>
 *
 * <p>The invocation of <code>syncPull(ctx, nResults)</code> has to return a result set of at most
 * nResults records. If the upstream (the previous steps) return more records, they have to be
 * returned by next call of <code>syncPull()</code>. The returned result set can have less than
 * nResults records ONLY if current step cannot produce any more records (eg. the upstream does not
 * have any more records)
 *
 * @author Luigi Dell'Aquila l.dellaquila - at - orientdb.com
 */
public interface OExecutionStepInternal {

  OExecutionStream start(OCommandContext ctx) throws OTimeoutException;

  void setPrevious(OExecutionStepInternal step);

  static String getIndent(OPrintContext ctx) {
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < ctx.getDepth(); i++) {
      for (int j = 0; j < ctx.getIdent(); j++) {
        result.append(" ");
      }
    }
    return result.toString();
  }

  default String prettyPrint(OPrintContext ctx) {
    String spaces = getIndent(ctx);
    return spaces + getClass().getSimpleName();
  }

  default String getName() {
    return getClass().getSimpleName();
  }

  default String getType() {
    return getClass().getSimpleName();
  }

  default String getTargetNode() {
    return "<local>";
  }

  default List<OExecutionStepInternal> getSubSteps() {
    return Collections.EMPTY_LIST;
  }

  default List<OInternalExecutionPlan> getSubExecutionPlans() {
    return Collections.EMPTY_LIST;
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
      for (OExecutionStepInternal substep : step.getSubSteps()) {
        serializedSubsteps.add(((OExecutionStepInternal) substep).serialize());
      }
      result.setProperty("subSteps", serializedSubsteps);
    }

    if (step.getSubExecutionPlans() != null && step.getSubExecutionPlans().size() > 0) {
      List<OResult> serializedSubPlans = new ArrayList<>();
      for (OInternalExecutionPlan substep : step.getSubExecutionPlans()) {
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
        OExecutionStepInternal subStep =
            (OExecutionStepInternal) Class.forName(className).newInstance();
        subStep.deserialize(serializedSub);
        step.getSubSteps().add(subStep);
      }
    }

    List<OResult> serializedPlans = serialized.getProperty("subExecutionPlans");
    if (serializedSubsteps != null) {
      for (OResult serializedSub : serializedPlans) {
        String className = serializedSub.getProperty(OInternalExecutionPlan.JAVA_TYPE);
        OInternalExecutionPlan subStep =
            (OInternalExecutionPlan) Class.forName(className).newInstance();
        subStep.deserialize(serializedSub);
        step.getSubExecutionPlans().add(subStep);
      }
    }
  }

  default OExecutionStepInternal copy(OCommandContext ctx) {
    throw new UnsupportedOperationException();
  }

  default boolean canBeCached() {
    return false;
  }

  default OResult toResult() {
    return toResult(new OToResultContextImpl());
  }

  default void serializeToResult(OResultInternal result, OToResultContext ctx) {}

  static void fillIndexes(OExecutionStepInternal step, Set<String> indexes) {
    for (OExecutionStepInternal chilStep : step.getSubSteps()) {
      fillIndexes(chilStep, indexes);
    }
    String index = step.toResult().getProperty("index");
    if (index != null) {
      indexes.add(index);
    }
  }

  default OResult toResult(OToResultContext ctx) {
    OResultInternal result = new OResultInternal();
    result.setProperty("name", getName());
    result.setProperty("type", getType());
    result.setProperty("targetNode", getType());
    result.setProperty(OInternalExecutionPlan.JAVA_TYPE, getClass().getName());
    result.setProperty("cost", ctx.getCost(this));
    result.setProperty(
        "subSteps",
        getSubSteps() == null
            ? null
            : getSubSteps().stream()
                .map(x -> ((OExecutionStepInternal) x).toResult(ctx))
                .collect(Collectors.toList()));
    String description = prettyPrint(new OPrintContexImpl(ctx.getContext(), 0, 3));
    result.setProperty("description", description);
    serializeToResult(result, ctx);
    return result;
  }
}
