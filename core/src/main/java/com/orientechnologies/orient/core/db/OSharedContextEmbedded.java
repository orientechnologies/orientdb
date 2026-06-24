package com.orientechnologies.orient.core.db;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.DISTRIBUTED_TRANSACTION_SEQUENCE_SET_SIZE;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.viewmanager.ViewManager;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.OIndexFactory;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.index.OIndexManagerShared;
import com.orientechnologies.orient.core.index.OIndexes;
import com.orientechnologies.orient.core.metadata.OSessionMetadata;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibraryImpl;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchemaEmbedded;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.core.metadata.security.OSecurityInternal;
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
import com.orientechnologies.orient.core.transaction.ODistributedSynchronizedSequence;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.tx.OTxMetadataHolderSyncOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/** Created by tglman on 13/06/17. */
public class OSharedContextEmbedded extends OSharedContext {
  private static final OLogger logger = OLogManager.instance().logger(OSharedContextEmbedded.class);
  protected static final OProfiler PROFILER = Orient.instance().getProfiler();

  protected OrientDBEmbedded orientDB;
  protected OStorage storage;
  protected OSchemaShared schema;
  protected OSecurityInternal security;
  protected OIndexManagerAbstract indexManager;
  protected OFunctionLibraryImpl functionLibrary;
  protected OSchedulerImpl scheduler;
  protected OSequenceLibraryImpl sequenceLibrary;
  protected OLiveQueryHook.OLiveQueryOps liveQueryOps;
  protected OLiveQueryHookV2.OLiveQueryOps liveQueryOpsV2;
  protected OStatementCache statementCache;
  protected OExecutionPlanCache executionPlanCache;
  protected OQueryStats queryStats;
  protected volatile boolean loaded = false;
  protected Map<String, Object> resources;
  protected OStringCache stringCache;
  private final AtomicInteger sessionCount = new AtomicInteger(0);
  private volatile long lastCloseTime = System.currentTimeMillis();
  protected Map<String, DistributedQueryContext> activeDistributedQueries;
  protected ViewManager viewManager;
  protected ODistributedSynchronizedSequence transactionSequence;

  public OSharedContextEmbedded(OStorage storage, OrientDBEmbedded orientDB) {
    this.orientDB = orientDB;
    this.storage = storage;
    storage.setConfigurationUpdateListener(
        update -> {
          for (OMetadataUpdateListener listener : browseListeners()) {
            listener.onStorageConfigurationUpdate(storage.getName(), update);
          }
        });
    init(storage);
  }

  protected void init(OStorage storage) {
    stringCache =
        new OStringCache(
            orientDB
                .getConfigurations()
                .getConfigurations()
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
            orientDB
                .getConfigurations()
                .getConfigurations()
                .getValueAsInteger(OGlobalConfiguration.STATEMENT_CACHE_SIZE));

    executionPlanCache =
        new OExecutionPlanCache(
            orientDB
                .getConfigurations()
                .getConfigurations()
                .getValueAsInteger(OGlobalConfiguration.STATEMENT_CACHE_SIZE));
    registerListener(executionPlanCache);

    queryStats = new OQueryStats();
    activeDistributedQueries = new HashMap<>();
    viewManager = new ViewManager(orientDB, storage.getName());
    int sequenceSize =
        orientDB
            .getConfigurations()
            .getConfigurations()
            .getValueAsInteger(DISTRIBUTED_TRANSACTION_SEQUENCE_SET_SIZE);

    transactionSequence = new ODistributedSynchronizedSequence(orientDB.getNodeId(), sequenceSize);
  }

  public synchronized void load(ODatabaseDocumentInternal database) {
    final long timer = PROFILER.startChrono();

    try {
      if (!loaded) {
        internalLoad(database);
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

  protected void internalLoad(ODatabaseDocumentInternal database) {
    transactionSequence.fill(getStorage().getLastMetadata());
    schema.load(database);
    schema.forceSnapshot(database);
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
  }

  @Override
  public synchronized void close() {
    internalClose();
    loaded = false;
  }

  protected void internalClose() {
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
  }

  @Override
  public void unload() {
    Optional<Future<Void>> future;
    synchronized (this) {
      future = internalUnload();
      loaded = false;
    }
    if (future.isPresent()) {
      try {
        future.get().get();
      } catch (InterruptedException | ExecutionException e) {
        logger.debug("Error on waiting freeze", e);
      }
    }
  }

  protected Optional<Future<Void>> internalUnload() {
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
    return Optional.empty();
  }

  public synchronized void reload(ODatabaseDocumentInternal database) {
    internalReload(database);
  }

  protected void internalReload(ODatabaseDocumentInternal database) {
    transactionSequence.fill(getStorage().getLastMetadata());
    schema.reload(database);
    indexManager.reload(database);
    // The Immutable snapshot should be after index and schema that require and before everything
    // else that use it
    schema.forceSnapshot(database);
    security.load(database);
    functionLibrary.load(database);
    sequenceLibrary.load(database);
    scheduler.load(database);
  }

  public synchronized void create(ODatabaseDocumentInternal database) {
    internalCreate(database);
    loaded = true;
  }

  protected void internalCreate(ODatabaseDocumentInternal database) {
    var status = transactionSequence.currentStatus();
    var metadata =
        new OTxMetadataHolderSyncOrder(new CountDownLatch(1), new OTransactionId(0, 0), status);
    getStorage().metadataOnly(metadata.metadata());
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
  }

  public Map<String, DistributedQueryContext> getActiveDistributedQueries() {
    return activeDistributedQueries;
  }

  public ViewManager getViewManager() {
    return viewManager;
  }

  public synchronized void reInit(OStorage storage, ODatabaseDocumentInternal database) {
    this.unload();
    this.storage = storage;
    this.init(storage);
    ((OSessionMetadata) database.getMetadata()).init(this);
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
                ORecordInternal.setIdentity(config, new ORecordId(-1, -1));
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
            if (record != null) {
              ORecordInternal.setVersion(value, record.getVersion());
            }
            session.save(value, "internal");
          } else {
            ORID recordId = session.save(value, "internal").getIdentity();
            storage.setProperty(propertyName, recordId.toString());
          }
          return null;
        });
  }

  public ODocument loadDistributedConfig(ODatabaseSession session) {
    return loadConfig(session, "ditributedConfig");
  }

  public ODistributedSynchronizedSequence getTransactionSequence() {
    return transactionSequence;
  }

  @Override
  public boolean isLoaded() {
    return loaded;
  }

  public OSchemaShared getSchema() {
    return schema;
  }

  public OSecurityInternal getSecurity() {
    return security;
  }

  public OIndexManagerAbstract getIndexManager() {
    return indexManager;
  }

  public OFunctionLibraryImpl getFunctionLibrary() {
    return functionLibrary;
  }

  public OSchedulerImpl getScheduler() {
    return scheduler;
  }

  public OSequenceLibraryImpl getSequenceLibrary() {
    return sequenceLibrary;
  }

  public OLiveQueryHook.OLiveQueryOps getLiveQueryOps() {
    return liveQueryOps;
  }

  public OLiveQueryHookV2.OLiveQueryOps getLiveQueryOpsV2() {
    return liveQueryOpsV2;
  }

  public OStatementCache getStatementCache() {
    return statementCache;
  }

  public OExecutionPlanCache getExecutionPlanCache() {
    return executionPlanCache;
  }

  public OQueryStats getQueryStats() {
    return queryStats;
  }

  public OStorage getStorage() {
    return storage;
  }

  public OrientDBInternal getOrientDB() {
    return orientDB;
  }

  public synchronized <T> T getResource(final String name, final Callable<T> factory) {
    if (resources == null) {
      resources = new HashMap<String, Object>();
    }
    @SuppressWarnings("unchecked")
    T resource = (T) resources.get(name);
    if (resource == null) {
      try {
        resource = factory.call();
      } catch (Exception e) {
        OException.wrapException(
            new ODatabaseException(String.format("instance creation for '%s' failed", name)), e);
      }
      resources.put(name, resource);
    }
    return resource;
  }

  public OStringCache getStringCache() {
    return this.stringCache;
  }

  public void startSession() {
    sessionCount.incrementAndGet();
  }

  public void endSession() {
    int count = sessionCount.decrementAndGet();
    assert count >= 0
        : "Amount of closed sessions in database "
            + storage.getName()
            + " is bigger than amount of open sessions";
    lastCloseTime = System.currentTimeMillis();
  }

  public int getSessionCount() {
    return sessionCount.get();
  }

  public long getLastCloseTime() {
    return lastCloseTime;
  }
}
