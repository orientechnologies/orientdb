package com.orientechnologies.agent.services.metrics.server.info;

import com.orientechnologies.agent.profiler.OMetricsRegistry;
import com.orientechnologies.agent.services.metrics.OGlobalMetrics;
import com.orientechnologies.agent.services.metrics.OrientDBMetric;
import com.orientechnologies.enterprise.server.OEnterpriseServer;

/**
 * Created by Enrico Risa on 18/07/2018.
 */
public class OrientDBServerInfoMetrics implements OrientDBMetric {

  private final OEnterpriseServer server;
  private final OMetricsRegistry  registry;

  public OrientDBServerInfoMetrics(OEnterpriseServer server, OMetricsRegistry registry) {

    this.server = server;
    this.registry = registry;
  }

  @Override
  public void start() {

    this.registry.gauge(OGlobalMetrics.SERVER_DATABASES.name, OGlobalMetrics.SERVER_DATABASES.description, () -> {
      final StringBuilder dbs = new StringBuilder(64);
      for (String dbName : server.getAvailableStorageNames().keySet()) {
        if (dbs.length() > 0)
          dbs.append(',');
        dbs.append(dbName);
      }
      return dbs.toString();
    });

  }

  @Override
  public void stop() {
    this.registry.remove(OGlobalMetrics.SERVER_DATABASES.name);
  }
}
