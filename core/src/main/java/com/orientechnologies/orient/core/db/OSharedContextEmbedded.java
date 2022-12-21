package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.viewmanager.ViewManager;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.OIndexFactory;
import com.orientechnologies.orient.core.index.OIndexManagerShared;
import com.orientechnologies.orient.core.index.OIndexes;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibraryImpl;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchemaEmbedded;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibraryImpl;
import com.orientechnologies.orient.core.query.live.OLiveQueryHook;
import com.orientechnologies.orient.core.query.live.OLiveQueryHookV2;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.schedule.OSchedulerImpl;
import com.orientechnologies.orient.core.sql.executor.OQueryStats;
import com.orientechnologies.orient.core.sql.parser.OExecutionPlanCache;
import com.orientechnologies.orient.core.sql.parser.OStatementCache;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import java.util.HashMap;
import java.util.Map;

/** Created by tglman on 13/06/17. */
public class OSharedContextEmbedded extends OSharedContext {

  protected Map<String, DistributedQueryContext> activeDistributedQueries;
  protected ViewManager viewManager;

  public OSharedContextEmbedded(OStorage storage, OrientDBEmbedded orientDB) {
    this.orientDB = orientDB;
    this.storage = storage;
    init(storage);
  }

  protected void init(OStorage storage) {
    stringCache =
        new OStringCache(
            storage
                .getConfiguration()
                .getContextConfiguration()
                .getValueAsInteger(OGlobalConfiguration.DB_STRING_CAHCE_SIZE));
    schema = new OSchemaEmbedded(this);
    security = orientDB.getSecuritySystem().newSecurity(storage.getName());
    indexManager = new OIndexManagerShared(storage);
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
    schema.reload(database);
    indexManager.reload();
    // The Immutable snapshot should be after index and schema that require and before everything
    // else that use it
    schema.forceSnapshot(database);
    security.load(database);
    functionLibrary.load(database);
    sequenceLibrary.load(database);
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

    // create geospatial classes
    try {
      OIndexFactory factory = OIndexes.getFactory(OClass.INDEX_TYPE.SPATIAL.toString(), "LUCENE");
      if (factory != null && factory instanceof ODatabaseLifecycleListener) {
        ((ODatabaseLifecycleListener) factory).onCreate(database);
      }
    } catch (OIndexException x) {
      // the index does not exist
    }

    viewManager.create();
    loaded = true;
  }

  public Map<String, DistributedQueryContext> getActiveDistributedQueries() {
    return activeDistributedQueries;
  }

  public ViewManager getViewManager() {
    return viewManager;
  }

  public synchronized void reInit(
      OAbstractPaginatedStorage storage2, ODatabaseDocumentInternal database) {
    this.close();
    this.storage = storage2;
    this.init(storage2);
    ((OMetadataDefault) database.getMetadata()).init(this);
    this.load(database);
  }

  public synchronized ODocument loadConfig(ODatabaseSession session, String name) {
    return (ODocument)
        OScenarioThreadLocal.executeAsDistributed(
            () -> {
              assert !session.getTransaction().isActive();
              String propertyName = "__config__" + name;
              String id = storage.getConfiguration().getProperty(propertyName);
              if (id != null) {
                ORecordId recordId = new ORecordId(id);
                ODocument config = session.load(recordId, null, false);
                ORecordInternal.setIdentity(config, null);
                return config;
              } else {
                return null;
              }
            });
  }

  /**
   * Store a configuration with a key, without checking eventual update version.
   *
   * @param session
   * @param name
   * @param value
   */
  public synchronized void saveConfig(ODatabaseSession session, String name, ODocument value) {
    OScenarioThreadLocal.executeAsDistributed(
        () -> {
          assert !session.getTransaction().isActive();
          String propertyName = "__config__" + name;
          String id = storage.getConfiguration().getProperty(propertyName);
          if (id != null) {
            ORecordId recordId = new ORecordId(id);
            ORecord record = session.load(recordId, null, false);
            ORecordInternal.setIdentity(value, recordId);
            ORecordInternal.setVersion(value, record.getVersion());
            session.save(value);
          } else {
            ORID recordId = session.save(value, "internal").getIdentity();
            ((OStorage) storage).setProperty(propertyName, recordId.toString());
          }
          return null;
        });
  }
}
