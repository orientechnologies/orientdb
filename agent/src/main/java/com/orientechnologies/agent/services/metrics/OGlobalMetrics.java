package com.orientechnologies.agent.services.metrics;

/**
 * Created by Enrico Risa on 16/07/2018.
 */
public enum OGlobalMetrics {
  SERVER_DATABASES("server.info.databases", "List of databases configured in Server"), SERVER_NETWORK_REQUESTS(
      "server.network.requests", "Stats of received network requests (HTTP/Binary)"), SERVER_NETWORK_SESSIONS(
      "server.network.sessions", "Number of opened sessions (HTTP/Binary)"), SERVER_RUNTIME_GC("server.runtime.gc",
      "Stats about GC"), SERVER_RUNTIME_MEMORY("server.runtime.memory", "Stats about memory usage"), SERVER_RUNTIME_THREADS(
      "server.runtime.threads", "Info about threads"), SERVER_RUNTIME_CPU("server.runtime.cpu", "Total cpu used by the process");

  public final String name;
  public final String description;

  OGlobalMetrics(String name, String description) {
    this.name = name;
    this.description = description;
  }
}
