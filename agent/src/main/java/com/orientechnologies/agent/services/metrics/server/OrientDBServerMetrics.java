package com.orientechnologies.agent.services.metrics.server;

import com.orientechnologies.agent.profiler.OMetricsRegistry;
import com.orientechnologies.agent.services.metrics.OrientDBMetric;
import com.orientechnologies.agent.services.metrics.OrientDBMetricSupport;
import com.orientechnologies.agent.services.metrics.server.info.OrientDBServerInfoMetrics;
import com.orientechnologies.agent.services.metrics.server.network.OrientDBServerNetworkMetrics;
import com.orientechnologies.agent.services.metrics.server.resources.OrientDBServerResourcesMetrics;
import com.orientechnologies.enterprise.server.OEnterpriseServer;

/** Created by Enrico Risa on 16/07/2018. */
public class OrientDBServerMetrics implements OrientDBMetric {

  private final OEnterpriseServer server;
  private final OMetricsRegistry registry;

  private OrientDBMetricSupport metricSupport = new OrientDBMetricSupport();

  public OrientDBServerMetrics(OEnterpriseServer server, OMetricsRegistry registry) {
    this.server = server;
    this.registry = registry;
    metricSupport.add(new OrientDBServerNetworkMetrics(server, registry));
    metricSupport.add(new OrientDBServerInfoMetrics(server, registry));
    metricSupport.add(new OrientDBServerResourcesMetrics(server, registry));
  }

  @Override
  public void start() {
    metricSupport.start();
  }

  @Override
  public void stop() {
    metricSupport.stop();
  }
}
