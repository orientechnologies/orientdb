package com.orientechnologies.agent.services.metrics.server.database;

import com.orientechnologies.agent.profiler.OMetricsRegistry;
import com.orientechnologies.agent.services.metrics.OrientDBMetric;
import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.enterprise.server.listener.OEnterpriseStorageListener;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OEnterpriseLocalPaginatedStorage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Enrico Risa on 20/07/2018.
 */
public class OrientDBDatabasesMetrics implements OrientDBMetric, OEnterpriseStorageListener {

  private final OEnterpriseServer server;
  private final OMetricsRegistry  registry;

  private final Map<String, OrientDBSingleDatabaseMetrics> storages = new ConcurrentHashMap<>();

  public OrientDBDatabasesMetrics(OEnterpriseServer server, OMetricsRegistry registry) {
    this.server = server;
    this.registry = registry;
  }

  @Override
  public void start() {
    server.registerDatabaseListener(this);
  }

  @Override
  public void onOpen(OEnterpriseLocalPaginatedStorage database) {
    OrientDBSingleDatabaseMetrics metrics = new OrientDBSingleDatabaseMetrics(this.server, this.registry, database);

    if (storages.putIfAbsent(database.getName(), metrics) == null) {
      metrics.start();
    }
  }

  @Override
  public void onDrop(OEnterpriseLocalPaginatedStorage database) {

    OrientDBSingleDatabaseMetrics db = storages.get(database.getName());
    if (db != null) {
      db.stop();
    }
  }

  @Override
  public void stop() {
    server.registerDatabaseListener(this);
    this.storages.forEach((k, v) -> v.stop());
  }
}
