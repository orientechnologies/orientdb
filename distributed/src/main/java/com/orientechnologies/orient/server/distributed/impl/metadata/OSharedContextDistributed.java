package com.orientechnologies.orient.server.distributed.impl.metadata;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.OMetadataUpdateListener;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.OSharedContextEmbedded;
import com.orientechnologies.orient.core.db.OStringCache;
import com.orientechnologies.orient.core.db.OrientDBDistributed;
import com.orientechnologies.orient.core.db.viewmanager.ViewManager;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.OIndexFactory;
import com.orientechnologies.orient.core.index.OIndexes;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibraryImpl;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibraryImpl;
import com.orientechnologies.orient.core.query.live.OLiveQueryHook;
import com.orientechnologies.orient.core.query.live.OLiveQueryHookV2;
import com.orientechnologies.orient.core.schedule.OSchedulerImpl;
import com.orientechnologies.orient.core.sql.executor.OQueryStats;
import com.orientechnologies.orient.core.sql.parser.OExecutionPlanCache;
import com.orientechnologies.orient.core.sql.parser.OStatementCache;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import java.util.HashMap;

/** Created by tglman on 22/06/17. */
public class OSharedContextDistributed extends OSharedContextEmbedded {

  public OSharedContextDistributed(OStorage storage, OrientDBDistributed orientDB) {
    super(storage, orientDB);
  }

  protected void init(OStorage storage) {
    stringCache =
        new OStringCache(
            storage
                .getConfiguration()
                .getContextConfiguration()
                .getValueAsInteger(OGlobalConfiguration.DB_STRING_CAHCE_SIZE));
    schema = new OSchemaDistributed(this);
    security = orientDB.getSecuritySystem().newSecurity(storage.getName());
    indexManager = new OIndexManagerDistributed(storage);
    functionLibrary = new OFunctionLibraryImpl();
    scheduler = new OSchedulerImpl(orientDB);
    sequenceLibrary = new OSequenceLibraryImpl();
    liveQueryOps = new OLiveQueryHook.OLiveQueryOps();
    liveQueryOpsV2 = new OLiveQueryHookV2.OLiveQueryOps();
    statementCache =
        new OStatementCache(
            storage
                .getConfiguration()
                .getContextConfiguration()
                .getValueAsInteger(OGlobalConfiguration.STATEMENT_CACHE_SIZE));

    executionPlanCache =
        new OExecutionPlanCache(
            storage
                .getConfiguration()
                .getContextConfiguration()
                .getValueAsInteger(OGlobalConfiguration.STATEMENT_CACHE_SIZE));
    this.registerListener(executionPlanCache);

    queryStats = new OQueryStats();
    activeDistributedQueries = new HashMap<>();
    ((OAbstractPaginatedStorage) storage)
        .setStorageConfigurationUpdateListener(
            update -> {
              for (OMetadataUpdateListener listener : browseListeners()) {
                listener.onStorageConfigurationUpdate(storage.getName(), update);
              }
            });

    this.viewManager = new ViewManager(orientDB, storage.getName());
  }

  public synchronized void load(ODatabaseDocumentInternal database) {
    OScenarioThreadLocal.executeAsDistributed(
        () -> {
          final long timer = PROFILER.startChrono();

          try {
            if (!loaded) {
              schema.load(database);
              indexManager.load(database);
              // The Immutable snapshot should be after index and schema that require and before
              // everything else that use it
              schema.forceSnapshot(database);
              security.load(database);
              functionLibrary.load(database);
              scheduler.load(database);
              sequenceLibrary.load(database);
              schema.onPostIndexManagement();
              viewManager.load();
              loaded = true;
            }
          } finally {
            PROFILER.stopChrono(
                PROFILER.getDatabaseMetric(database.getName(), "metadata.load"),
                "Loading of database metadata",
                timer,
                "db.*.metadata.load");
          }
          return null;
        });
  }

  @Override
  public synchronized void close() {
    stringCache.close();
    viewManager.close();
    schema.close();
    security.close();
    indexManager.close();
    functionLibrary.close();
    scheduler.close();
    sequenceLibrary.close();
    statementCache.clear();
    executionPlanCache.invalidate();
    liveQueryOps.close();
    liveQueryOpsV2.close();
    activeDistributedQueries.values().forEach(x -> x.close());
    loaded = false;
  }

  public synchronized void reload(ODatabaseDocumentInternal database) {
    OScenarioThreadLocal.executeAsDistributed(
        () -> {
          schema.reload(database);
          indexManager.reload();
          // The Immutable snapshot should be after index and schema that require and before
          // everything else that use it
          schema.forceSnapshot(database);
          security.load(database);
          functionLibrary.load(database);
          sequenceLibrary.load(database);
          scheduler.load(database);
          return null;
        });
  }

  public synchronized void create(ODatabaseDocumentInternal database) {
    OScenarioThreadLocal.executeAsDistributed(
        () -> {
          schema.create(database);
          indexManager.create(database);
          security.create(database);
          functionLibrary.create(database);
          sequenceLibrary.create(database);
          security.createClassTrigger(database);
          scheduler.create(database);

          // CREATE BASE VERTEX AND EDGE CLASSES
          schema.createClass(database, "V");
          schema.createClass(database, "E");

          // create geospatial classes
          try {
            OIndexFactory factory =
                OIndexes.getFactory(OClass.INDEX_TYPE.SPATIAL.toString(), "LUCENE");
            if (factory != null && factory instanceof ODatabaseLifecycleListener) {
              ((ODatabaseLifecycleListener) factory).onCreate(database);
            }
          } catch (OIndexException x) {
            // the index does not exist
          }

          viewManager.create();
          schema.forceSnapshot(database);
          loaded = true;
          return null;
        });
  }

  public ViewManager getViewManager() {
    return viewManager;
  }
}
