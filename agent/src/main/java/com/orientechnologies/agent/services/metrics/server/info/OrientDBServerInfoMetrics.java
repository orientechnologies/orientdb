package com.orientechnologies.agent.services.metrics.server.info;

import com.orientechnologies.agent.profiler.OMetricsRegistry;
import com.orientechnologies.agent.profiler.metrics.OMetric;
import com.orientechnologies.agent.profiler.metrics.OMetricSet;
import com.orientechnologies.agent.services.metrics.OGlobalMetrics;
import com.orientechnologies.agent.services.metrics.OrientDBMetric;
import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.core.OConstants;
import java.util.HashMap;
import java.util.Map;

/** Created by Enrico Risa on 18/07/2018. */
public class OrientDBServerInfoMetrics implements OrientDBMetric {

  private final OEnterpriseServer server;
  private final OMetricsRegistry registry;

  protected final int processors = Runtime.getRuntime().availableProcessors();

  public OrientDBServerInfoMetrics(OEnterpriseServer server, OMetricsRegistry registry) {

    this.server = server;
    this.registry = registry;
  }

  @Override
  public void start() {

    this.registry.registerAll(
        OGlobalMetrics.SERVER_INFO.name,
        new OMetricSet() {
          @Override
          public Map<String, OMetric> getMetrics() {

            Map<String, OMetric> metrics = new HashMap<>();

            metrics.put(
                "databases",
                registry.newGauge(
                    "databases",
                    "List of databases",
                    OrientDBServerInfoMetrics.this::getDatabasesList));

            metrics.put("cpus", registry.newGauge("cpus", "Number of CPUs", () -> processors));
            metrics.put(
                "osName",
                registry.newGauge(
                    "osName", "Operating System name", () -> System.getProperty("os.name")));
            metrics.put(
                "osVersion",
                registry.newGauge(
                    "osVersion",
                    "Operating System version",
                    () -> System.getProperty("os.version")));
            metrics.put(
                "osArch",
                registry.newGauge(
                    "osArch",
                    "Operating System architecture",
                    () -> System.getProperty("os.arch")));
            metrics.put(
                "javaVendor",
                registry.newGauge(
                    "javaVendor", "Java Vendor", () -> System.getProperty("java.vendor")));
            metrics.put(
                "javaVersion",
                registry.newGauge(
                    "javaVersion", "Java Version", () -> System.getProperty("java.version")));
            metrics.put(
                "version",
                registry.newGauge("version", "OrientDB Version", () -> OConstants.getRawVersion()));

            return metrics;
          }

          @Override
          public String prefix() {
            return OGlobalMetrics.SERVER_INFO.name;
          }

          @Override
          public String getName() {
            return OGlobalMetrics.SERVER_INFO.name;
          }

          @Override
          public String getDescription() {
            return OGlobalMetrics.SERVER_INFO.description;
          }
        });
  }

  private String getDatabasesList() {
    final StringBuilder dbs = new StringBuilder(64);
    for (String dbName : server.getAvailableStorageNames().keySet()) {
      if (dbs.length() > 0) dbs.append(',');
      dbs.append(dbName);
    }
    return dbs.toString();
  }

  @Override
  public void stop() {
    this.registry.remove(OGlobalMetrics.SERVER_INFO.name);
  }
}
