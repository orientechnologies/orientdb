package com.orientechnologies.agent.services.metrics.server.database;

import com.orientechnologies.agent.profiler.OMetricsRegistry;
import com.orientechnologies.agent.services.metrics.OrientDBMetric;
import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.enterprise.server.listener.OEnterpriseStorageListener;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OEnterpriseLocalPaginatedStorage;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Created by Enrico Risa on 20/07/2018. */
public class OrientDBDatabasesMetrics implements OrientDBMetric, OEnterpriseStorageListener {

  private final OEnterpriseServer server;
  private final OMetricsRegistry registry;

  private final Map<String, OrientDBSingleDatabaseMetrics> storages = new ConcurrentHashMap<>();
  private final Map<String, OrientDBDatabaseQueryMetrics> queries = new ConcurrentHashMap<>();

  public OrientDBDatabasesMetrics(OEnterpriseServer server, OMetricsRegistry registry) {
    this.server = server;
    this.registry = registry;
    server.registerDatabaseListener(this);
  }

  @Override
  public void start() {}

  @Override
  public void onOpen(OEnterpriseLocalPaginatedStorage database) {
    OrientDBSingleDatabaseMetrics metrics =
        new OrientDBSingleDatabaseMetrics(this.server, this.registry, database);
    if (storages.putIfAbsent(database.getName(), metrics) == null) {
      metrics.start();
    }
    OrientDBDatabaseQueryMetrics queryMetrics =
        new OrientDBDatabaseQueryMetrics(this.server, this.registry, database.getName());
    if (queries.putIfAbsent(database.getName(), queryMetrics) == null) {
      queryMetrics.start();
    }
  }

  @Override
  public void onDrop(OEnterpriseLocalPaginatedStorage database) {

    OrientDBSingleDatabaseMetrics db = storages.get(database.getName());
    if (db != null) {
      db.stop();
    }

    OrientDBDatabaseQueryMetrics q = queries.get(database.getName());
    if (q != null) {
      q.stop();
    }
  }

  @Override
  public void stop() {
    server.unRegisterDatabaseListener(this);

    this.storages.forEach((k, v) -> v.stop());

    this.queries.forEach((k, v) -> v.stop());
  }

  @Override
  public void onCommandStart(ODatabase database, OResultSet result) {}

  @Override
  public void onCommandEnd(ODatabase database, OResultSet result) {
    OrientDBDatabaseQueryMetrics queryMetrics = queries.get(database.getName());
    if (queryMetrics != null) {

      queryMetrics.onResultSet(result);
    }
  }
}
