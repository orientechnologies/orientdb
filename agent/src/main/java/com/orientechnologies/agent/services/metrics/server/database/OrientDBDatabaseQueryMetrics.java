package com.orientechnologies.agent.services.metrics.server.database;

import com.orientechnologies.agent.profiler.OMetricsRegistry;
import com.orientechnologies.agent.profiler.metrics.OHistogram;
import com.orientechnologies.agent.services.metrics.OGlobalMetrics;
import com.orientechnologies.agent.services.metrics.OrientDBMetric;
import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

/** Created by Enrico Risa on 20/07/2018. */
public class OrientDBDatabaseQueryMetrics implements OrientDBMetric {

  private final OEnterpriseServer server;
  private final OMetricsRegistry registry;
  private String storage;

  public OrientDBDatabaseQueryMetrics(
      OEnterpriseServer server, OMetricsRegistry registry, String database) {
    this.server = server;
    this.registry = registry;
    this.storage = database;
  }

  @Override
  public void start() {}

  @Override
  public synchronized void stop() {

    registry.removeStartWith(String.format("db.%s.query", this.storage));
  }

  public void onResultSet(OResultSet resultSet) {

    server
        .getQueryInfo(resultSet)
        .ifPresent(
            (it) -> {
              OHistogram histogram =
                  registry.histogram(
                      String.format(
                          OGlobalMetrics.DATABASE_QUERY_STATS.name,
                          this.storage,
                          it.getLanguage(),
                          it.getStatement()),
                      OGlobalMetrics.DATABASE_CREATE_OPS.description);
              histogram.update(it.getElapsedTimeMillis());
            });
  }
}
