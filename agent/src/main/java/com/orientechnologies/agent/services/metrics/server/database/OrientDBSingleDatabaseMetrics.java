package com.orientechnologies.agent.services.metrics.server.database;

import com.orientechnologies.agent.profiler.OMetricsRegistry;
import com.orientechnologies.agent.profiler.metrics.OMeter;
import com.orientechnologies.agent.services.metrics.OGlobalMetrics;
import com.orientechnologies.agent.services.metrics.OrientDBMetric;
import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OEnterpriseLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OEnterpriseStorageOperationListener;
import java.util.List;

/** Created by Enrico Risa on 20/07/2018. */
public class OrientDBSingleDatabaseMetrics
    implements OrientDBMetric, OEnterpriseStorageOperationListener {

  private final OEnterpriseServer server;
  private final OMetricsRegistry registry;
  private OEnterpriseLocalPaginatedStorage storage;

  private OMeter createOperation;
  private OMeter updateOperation;
  private OMeter readOperation;
  private OMeter deleteOperation;
  private OMeter commitOperation;
  private OMeter rollbackOperation;

  public OrientDBSingleDatabaseMetrics(
      OEnterpriseServer server,
      OMetricsRegistry registry,
      OEnterpriseLocalPaginatedStorage storage) {
    this.server = server;
    this.registry = registry;
    this.storage = storage;
  }

  @Override
  public void start() {

    this.createOperation =
        registry.meter(
            String.format(OGlobalMetrics.DATABASE_CREATE_OPS.name, this.storage.getName()),
            OGlobalMetrics.DATABASE_CREATE_OPS.description);

    this.readOperation =
        registry.meter(
            String.format(OGlobalMetrics.DATABASE_READ_OPS.name, this.storage.getName()),
            OGlobalMetrics.DATABASE_READ_OPS.description);

    this.updateOperation =
        registry.meter(
            String.format(OGlobalMetrics.DATABASE_UPDATE_OPS.name, this.storage.getName()),
            OGlobalMetrics.DATABASE_UPDATE_OPS.description);

    this.deleteOperation =
        registry.meter(
            String.format(OGlobalMetrics.DATABASE_DELETE_OPS.name, this.storage.getName()),
            OGlobalMetrics.DATABASE_DELETE_OPS.description);

    this.commitOperation =
        registry.meter(
            String.format(OGlobalMetrics.DATABASE_COMMIT_OPS.name, this.storage.getName()),
            OGlobalMetrics.DATABASE_COMMIT_OPS.description);

    this.rollbackOperation =
        registry.meter(
            String.format(OGlobalMetrics.DATABASE_ROLLBACK_OPS.name, this.storage.getName()),
            OGlobalMetrics.DATABASE_ROLLBACK_OPS.description);

    this.storage.registerStorageListener(this);
  }

  @Override
  public void stop() {

    this.storage.unRegisterStorageListener(this);

    registry.remove(String.format(OGlobalMetrics.DATABASE_CREATE_OPS.name, this.storage.getName()));
    registry.remove(String.format(OGlobalMetrics.DATABASE_READ_OPS.name, this.storage.getName()));
    registry.remove(String.format(OGlobalMetrics.DATABASE_UPDATE_OPS.name, this.storage.getName()));
    registry.remove(String.format(OGlobalMetrics.DATABASE_DELETE_OPS.name, this.storage.getName()));
    registry.remove(String.format(OGlobalMetrics.DATABASE_COMMIT_OPS.name, this.storage.getName()));
    registry.remove(
        String.format(OGlobalMetrics.DATABASE_ROLLBACK_OPS.name, this.storage.getName()));
  }

  @Override
  public void onCommit(List<ORecordOperation> operations) {

    for (ORecordOperation operation : operations) {
      switch (operation.type) {
        case ORecordOperation.CREATED:
          createOperation.mark();
          break;
        case ORecordOperation.UPDATED:
          updateOperation.mark();
          break;
        case ORecordOperation.DELETED:
          deleteOperation.mark();
          break;
      }
    }
    commitOperation.mark();
  }

  @Override
  public void onRollback() {
    this.rollbackOperation.mark();
  }

  @Override
  public void onRead() {
    readOperation.mark();
  }
}
