package com.orientechnologies.agent.services.metrics;

/** Created by Enrico Risa on 16/07/2018. */
public enum OGlobalMetrics {
  SERVER_INFO("server.info", "Info about OrientDB Server"),
  SERVER_NETWORK_REQUESTS(
      "server.network.requests", "Stats of received network requests (HTTP/Binary)"),
  SERVER_NETWORK_SESSIONS("server.network.sessions", "Number of opened sessions (HTTP/Binary)"),
  SERVER_NETWORK_SOCKETS("server.network.sockets", "Number of opened sockets (HTTP/Binary)"),
  SERVER_RUNTIME_GC("server.runtime.gc", "Stats about GC"),
  SERVER_RUNTIME_MEMORY("server.runtime.memory", "Stats about memory usage"),
  SERVER_RUNTIME_THREADS("server.runtime.threads", "Info about threads"),
  SERVER_RUNTIME_CPU("server.runtime.cpu", "Total cpu used by the process"),
  SERVER_RUNTIME_DISK_CACHE("server.runtime.diskCache", "Stats about Disk Cache"),
  SERVER_DISK_SPACE("server.disk.space", "Stats about disk space"),
  DATABASE_READ_OPS("db.%s.readOps", "Stats on read operations"),
  DATABASE_CREATE_OPS("db.%s.createOps", "Stats on create operations"),
  DATABASE_UPDATE_OPS("db.%s.updateOps", "Stats on update operations"),
  DATABASE_QUERY_STATS("db.%s.query.%s.%s", "Stats on queries"),
  DATABASE_DELETE_OPS("db.%s.deleteOps", "Stats on delete operations"),
  DATABASE_COMMIT_OPS("db.%s.commitOps", "Stats on commit operations"),
  DATABASE_ROLLBACK_OPS("db.%s.rollbackOps", "Stats on rollback operations");

  public final String name;
  public final String description;

  OGlobalMetrics(String name, String description) {
    this.name = name;
    this.description = description;
  }
}
