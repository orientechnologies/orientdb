package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.cache.OCommandCacheSoftRefs;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.viewmanager.ViewManager;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.OIndexFactory;
import com.orientechnologies.orient.core.index.OIndexManagerShared;
import com.orientechnologies.orient.core.index.OIndexes;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibraryImpl;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchemaEmbedded;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibraryImpl;
import com.orientechnologies.orient.core.query.live.OLiveQueryHook;
import com.orientechnologies.orient.core.query.live.OLiveQueryHookV2;
import com.orientechnologies.orient.core.schedule.OSchedulerImpl;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.sql.executor.OQueryStats;
import com.orientechnologies.orient.core.sql.parser.OExecutionPlanCache;
import com.orientechnologies.orient.core.sql.parser.OStatementCache;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by tglman on 13/06/17.
 */
public class OSharedContextEmbedded extends OSharedContext {

  private Map<String, DistributedQueryContext> activeDistributedQueries;

  private ViewManager viewManager;

  public OSharedContextEmbedded(OStorage storage, OrientDBEmbedded orientDB) {
    this.orientDB = orientDB;
    this.storage = storage;
    schema = new OSchemaEmbedded(this);
    security = OSecurityManager.instance().newSecurity();
    indexManager = new OIndexManagerShared(storage);
    functionLibrary = new OFunctionLibraryImpl();
    scheduler = new OSchedulerImpl();
    sequenceLibrary = new OSequenceLibraryImpl();
    liveQueryOps = new OLiveQueryHook.OLiveQueryOps();
    liveQueryOpsV2 = new OLiveQueryHookV2.OLiveQueryOps();
    commandCache = new OCommandCacheSoftRefs(storage.getUnderlying());
    statementCache = new OStatementCache(
        storage.getConfiguration().getContextConfiguration().getValueAsInteger(OGlobalConfiguration.STATEMENT_CACHE_SIZE));

    executionPlanCache = new OExecutionPlanCache(
        storage.getConfiguration().getContextConfiguration().getValueAsInteger(OGlobalConfiguration.STATEMENT_CACHE_SIZE));
    this.registerListener(executionPlanCache);

    queryStats = new OQueryStats();
    activeDistributedQueries = new HashMap<>();
    ((OAbstractPaginatedStorage) storage).setStorageConfigurationUpdateListener(update -> {
      for (OMetadataUpdateListener listener : browseListeners()) {
        listener.onStorageConfigurationUpdate(storage.getName(), update);
      }
    });

    this.viewManager = new ViewManager(orientDB, storage.getName());
  }

  public synchronized void load(ODatabaseDocumentInternal database) {
    final long timer = PROFILER.startChrono();

    try {
      if (!loaded) {
        schema.load(database);
        indexManager.load(database);
        //The Immutable snapshot should be after index and schema that require and before everything else that use it
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
      PROFILER
          .stopChrono(PROFILER.getDatabaseMetric(database.getStorage().getName(), "metadata.load"), "Loading of database metadata",
              timer, "db.*.metadata.load");
    }
  }

  @Override
  public synchronized void close() {
    viewManager.close();
    schema.close();
    security.close();
    indexManager.close();
    functionLibrary.close();
    scheduler.close();
    sequenceLibrary.close();
    commandCache.shutdown();
    statementCache.clear();
    executionPlanCache.invalidate();
    liveQueryOps.close();
    liveQueryOpsV2.close();
    activeDistributedQueries.values().forEach(x -> x.close());
    loaded =false;
  }

  public synchronized void reload(ODatabaseDocumentInternal database) {
    schema.reload(database);
    indexManager.reload();
    //The Immutable snapshot should be after index and schema that require and before everything else that use it
    schema.forceSnapshot(database);
    security.load(database);
    functionLibrary.load(database);
    sequenceLibrary.load(database);
    commandCache.clear();
    scheduler.load(database);
  }

  public synchronized void create(ODatabaseDocumentInternal database) {
    schema.create(database);
    indexManager.create(database);
    security.create(database);
    functionLibrary.create(database);
    sequenceLibrary.create(database);
    security.createClassTrigger(database);
    scheduler.create(database);
    schema.forceSnapshot(database);

    // CREATE BASE VERTEX AND EDGE CLASSES
    schema.createClass(database, "V");
    schema.createClass(database, "E");

    //create geospatial classes
    try {
      OIndexFactory factory = OIndexes.getFactory(OClass.INDEX_TYPE.SPATIAL.toString(), "LUCENE");
      if (factory != null && factory instanceof ODatabaseLifecycleListener) {
        ((ODatabaseLifecycleListener) factory).onCreate(database);
      }
    } catch (OIndexException x) {
      //the index does not exist
    }

    loaded = true;
  }

  public Map<String, DistributedQueryContext> getActiveDistributedQueries() {
    return activeDistributedQueries;
  }

  public ViewManager getViewManager() {
    return viewManager;
  }


}
