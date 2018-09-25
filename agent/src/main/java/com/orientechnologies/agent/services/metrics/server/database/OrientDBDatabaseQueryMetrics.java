package com.orientechnologies.agent.services.metrics.server.database;

import com.orientechnologies.agent.profiler.OMetricsRegistry;
import com.orientechnologies.agent.profiler.metrics.OHistogram;
import com.orientechnologies.agent.services.metrics.OGlobalMetrics;
import com.orientechnologies.agent.services.metrics.OrientDBMetric;
import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OInternalExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.parser.OLocalResultSet;
import com.orientechnologies.orient.core.sql.parser.OLocalResultSetLifecycleDecorator;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Enrico Risa on 20/07/2018.
 */
public class OrientDBDatabaseQueryMetrics implements OrientDBMetric {

  private final OEnterpriseServer server;
  private final OMetricsRegistry  registry;
  private       String            storage;

  private Map<String, OHistogram> queries = new ConcurrentHashMap<>();

  public OrientDBDatabaseQueryMetrics(OEnterpriseServer server, OMetricsRegistry registry, String database) {
    this.server = server;
    this.registry = registry;
    this.storage = database;
  }

  @Override
  public void start() {

  }

  @Override
  public void stop() {

    queries.forEach((s, v) -> registry.remove(String.format(OGlobalMetrics.DATABASE_QUERY_STATS.name, this.storage, s)));
  }

  public void onResultSet(OResultSet resultSet) {

    Optional<OExecutionPlan> plan = resultSet.getExecutionPlan();

    String query = plan.map((p -> {
      String q = "";
      if (p instanceof OInternalExecutionPlan) {
        String stm = ((OInternalExecutionPlan) p).getStatement();
        if (stm != null) {
          q = stm;
        }
      }
      return q;
    })).orElse("");

    if (resultSet instanceof OLocalResultSetLifecycleDecorator) {
      OResultSet oResultSet = ((OLocalResultSetLifecycleDecorator) resultSet).getInternal();
      if (oResultSet instanceof OLocalResultSet) {
        long totalExecutionTime = ((OLocalResultSet) oResultSet).getTotalExecutionTime();

        OHistogram histogram = queries.computeIfAbsent(query, (k) -> registry
            .histogram(String.format(OGlobalMetrics.DATABASE_QUERY_STATS.name, this.storage, query),
                OGlobalMetrics.DATABASE_CREATE_OPS.description));

        histogram.update(totalExecutionTime);

      }
    }

  }

}
