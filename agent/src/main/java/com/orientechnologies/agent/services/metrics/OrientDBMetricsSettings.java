package com.orientechnologies.agent.services.metrics;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.orientechnologies.agent.services.metrics.reporters.Reporters;

/** Created by Enrico Risa on 13/07/2018. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrientDBMetricsSettings {

  public Boolean enabled = false;

  public ServerMetricsSettings server = new ServerMetricsSettings();
  public DatabaseMetricsSettings database = new DatabaseMetricsSettings();
  public ClusterMetricsSettings cluster = new ClusterMetricsSettings();
  public Reporters reporters = new Reporters();

  public OrientDBMetricsSettings() {}

  public class ServerMetricsSettings {
    public Boolean enabled = false;

    public ServerMetricsSettings() {}
  }

  public class DatabaseMetricsSettings {
    public Boolean enabled = false;

    public DatabaseMetricsSettings() {}
  }

  public class ClusterMetricsSettings {
    public Boolean enabled = false;

    public ClusterMetricsSettings() {}
  }
}
