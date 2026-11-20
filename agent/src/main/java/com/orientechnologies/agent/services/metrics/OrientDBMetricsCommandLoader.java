package com.orientechnologies.agent.services.metrics;

import com.orientechnologies.agent.profiler.OMetricsRegistry;
import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommand;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandLoader;
import java.util.Map;

public class OrientDBMetricsCommandLoader implements OServerCommandLoader {

  private OEnterpriseServer enterpriseServer;
  private OMetricsRegistry registry;
  private OrientDBMetricsService service;

  public OrientDBMetricsCommandLoader(
      OEnterpriseServer enterpriseServer,
      OMetricsRegistry registry,
      OrientDBMetricsService settings) {
    this.enterpriseServer = enterpriseServer;
    this.registry = registry;
    this.service = settings;
  }

  @Override
  public String getCommandClass() {
    return OrientDBMetricsCommand.class.getName();
  }

  @Override
  public String getName() {
    return "metrics";
  }

  @Override
  public OServerCommand newInstance(String pattern, Map<String, String> configs) {
    return new OrientDBMetricsCommand(enterpriseServer, registry, service);
  }
}
