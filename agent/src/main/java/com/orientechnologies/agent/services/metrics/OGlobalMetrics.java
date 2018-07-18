package com.orientechnologies.agent.services.metrics;

/**
 * Created by Enrico Risa on 16/07/2018.
 */
public enum OGlobalMetrics {
  SERVER_DATABASES("server.info.databases", "List of databases configured in Server"), SERVER_NETWORK_REQUESTS(
      "server.network.requests", "Stats of received network requests (HTTP/Binary)"), SERVER_NETWORK_SESSIONS(
      "server.network.sessions", "Number of opened sessions (HTTP/Binary)");

  public final String name;
  public final String description;

  OGlobalMetrics(String name, String description) {
    this.name = name;
    this.description = description;
  }
}
