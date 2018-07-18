package com.orientechnologies.agent.profiler.source;

import com.orientechnologies.agent.profiler.OMetricsRegistry;
import com.orientechnologies.orient.server.OServer;

/**
 * Created by Enrico Risa on 13/07/2018.
 */
public class ServerMetrics {

  OServer          server;
  OMetricsRegistry registry;

  public ServerMetrics(OServer server, OMetricsRegistry registry) {
    this.server = server;
    this.registry = registry;
  }
}
