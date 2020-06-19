package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.sql.executor.FetchFromIndexStep;
import com.orientechnologies.orient.core.sql.executor.GlobalLetQueryStep;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import java.util.Map;
import java.util.Optional;
import org.apache.tinkerpop.gremlin.orientdb.executor.OGremlinResultSet;

public class OrientGraphQuery implements OrientGraphBaseQuery {

  protected final Map<String, Object> params;
  protected final String query;
  private final Integer target;

  public OrientGraphQuery(String query, Map<String, Object> params, Integer target) {
    this.query = query;
    this.params = params;
    this.target = target;
  }

  public String getQuery() {
    return query;
  }

  public Map<String, Object> getParams() {
    return params;
  }

  public OGremlinResultSet execute(OGraph graph) {
    return graph.querySql(this.query, this.params);
  }

  public Optional<OExecutionPlan> explain(OGraph graph) {
    return graph
        .querySql(String.format("EXPLAIN %s", query), params)
        .getRawResultSet()
        .getExecutionPlan();
  }

  public int usedIndexes(OGraph graph) {

    OExecutionPlan executionPlan = this.explain(graph).get();
    if (target > 1) {
      return executionPlan.getSteps().stream()
          .filter(step -> (step instanceof GlobalLetQueryStep))
          .map(
              s -> {
                GlobalLetQueryStep subStep = (GlobalLetQueryStep) s;
                return (int)
                    subStep.getSubExecutionPlans().stream()
                        .filter(
                            plan ->
                                plan.getSteps().stream()
                                        .filter((step) -> step instanceof FetchFromIndexStep)
                                        .count()
                                    > 0)
                        .count();
              })
          .reduce(0, (a, b) -> a + b);
    } else {
      return (int)
          executionPlan.getSteps().stream()
              .filter((step) -> step instanceof FetchFromIndexStep)
              .count();
    }
  }
}
