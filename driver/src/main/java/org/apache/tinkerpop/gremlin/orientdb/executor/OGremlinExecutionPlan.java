package org.apache.tinkerpop.gremlin.orientdb.executor;

import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OExecutionStep;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import java.util.ArrayList;
import java.util.List;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalExplanation;

/** Created by Enrico Risa on 25/05/2017. */
public class OGremlinExecutionPlan implements OExecutionPlan {

  TraversalExplanation explanation;

  public OGremlinExecutionPlan(TraversalExplanation explanation) {
    this.explanation = explanation;
  }

  @Override
  public List<OExecutionStep> getSteps() {
    return new ArrayList<>();
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    return explanation.prettyPrint();
  }

  @Override
  public OResult toResult() {
    OResultInternal result = new OResultInternal();
    result.setProperty("type", "GremlinExecutionPlan");
    result.setProperty("javaType", getClass().getName());
    result.setProperty("stmText", null);
    result.setProperty("cost", null);
    result.setProperty("prettyPrint", prettyPrint(0, 2));

    return result;
  }
}
