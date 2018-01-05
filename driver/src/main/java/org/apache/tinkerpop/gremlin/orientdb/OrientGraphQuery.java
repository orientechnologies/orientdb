package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.sql.executor.FetchFromIndexStep;
import com.orientechnologies.orient.core.sql.executor.GlobalLetQueryStep;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import org.apache.tinkerpop.gremlin.orientdb.executor.OGremlinResultSet;

import java.util.Map;
import java.util.Optional;

public class OrientGraphQuery implements OrientGraphBaseQuery {

  protected final Map<String, Object> params;
  protected final String              query;

  public OrientGraphQuery(String query, Map<String, Object> params) {
    this.query = query;
    this.params = params;
  }

  public String getQuery() {
    return query;
  }

  public Map<String, Object> getParams() {
    return params;
  }

  public OGremlinResultSet execute(OGraph graph) {
    return graph.executeSql(this.query, this.params);
  }

  public Optional<OExecutionPlan> explain(OGraph graph) {
    return graph.executeSql(String.format("EXPLAIN %s", query), params).getRawResultSet().getExecutionPlan();
  }

  public int usedIndexes(OGraph graph) {
    return this.explain(graph).get().getSteps().stream().filter(step -> step instanceof GlobalLetQueryStep).map(s -> {
      GlobalLetQueryStep subStep = (GlobalLetQueryStep) s;
      return (int) subStep.getSubExecutionPlans().stream()
          .filter(plan -> plan.getSteps().stream().filter((step) -> step instanceof FetchFromIndexStep).count() > 0).count();
    }).reduce(0, (a, b) -> a + b);
  }

}
