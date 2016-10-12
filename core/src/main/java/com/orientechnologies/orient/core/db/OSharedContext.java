package com.orientechnologies.orient.core.db;

import com.orientechnologies.common.concur.resource.OCloseable;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.cache.OCommandCache;
import com.orientechnologies.orient.core.cache.OCommandCacheSoftRefs;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibraryImpl;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibraryImpl;
import com.orientechnologies.orient.core.query.live.OLiveQueryHook;
import com.orientechnologies.orient.core.schedule.OSchedulerImpl;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.sql.executor.OQueryStats;
import com.orientechnologies.orient.core.sql.parser.OStatementCache;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageProxy;

/**
 * Created by tglman on 15/06/16.
 */
public class OSharedContext implements OCloseable {
  protected static final OProfiler PROFILER = Orient.instance().getProfiler();
  private OSchemaShared                schema;
  private OSecurity                    security;
  private OIndexManagerAbstract        indexManager;
  private OFunctionLibraryImpl         functionLibrary;
  private OSchedulerImpl               scheduler;
  private OSequenceLibraryImpl         sequenceLibrary;
  private OLiveQueryHook.OLiveQueryOps liveQueryOps;
  private OCommandCache                commandCache;
  private OStatementCache              statementCache;
  private OQueryStats                  queryStats;

  private volatile boolean loaded = false;

  public OSharedContext(OStorage storage) {
    schema = new OSchemaShared(storage.getComponentsFactory().classesAreDetectedByClusterId());

    security = OSecurityManager.instance().newSecurity();

    if (storage instanceof OStorageProxy)
      indexManager = new OIndexManagerRemote();
    else
      indexManager = new OIndexManagerShared();

    functionLibrary = new OFunctionLibraryImpl();
    scheduler = new OSchedulerImpl();
    sequenceLibrary = new OSequenceLibraryImpl();
    liveQueryOps = new OLiveQueryHook.OLiveQueryOps();
    commandCache = new OCommandCacheSoftRefs(storage);
    statementCache = new OStatementCache(OGlobalConfiguration.STATEMENT_CACHE_SIZE.getValueAsInteger());
    queryStats = new OQueryStats();
  }

  public synchronized void load(ODatabaseDocumentInternal database) {
    final long timer = PROFILER.startChrono();

    try {
      if (!loaded) {
        schema.load(database);
        security.load();
        indexManager.load(database);
        if (!(database.getStorage() instanceof OStorageProxy)) {
          functionLibrary.load(database);
          scheduler.load(database);
        }
        sequenceLibrary.load(database);
        schema.onPostIndexManagement();
        loaded = true;
      }
    } finally {
      PROFILER
          .stopChrono(PROFILER.getDatabaseMetric(database.getStorage().getName(), "metadata.load"), "Loading of database metadata", timer,
              "db.*.metadata.load");
    }
  }

  @Override
  public synchronized void close() {
    schema.close();
    security.close(false);
    indexManager.close();
    functionLibrary.close();
    scheduler.close();
    sequenceLibrary.close();
    if (commandCache != null) {
      commandCache.clear();
      commandCache.shutdown();
    }
    if (liveQueryOps != null)
      liveQueryOps.close();
  }

  public synchronized void reload(ODatabaseDocumentInternal database) {
    schema.reload();
    indexManager.reload();
    security.load();
    functionLibrary.load(database);
    sequenceLibrary.load(database);
    commandCache.clear();
    scheduler.load(database);
  }

  public synchronized void create(ODatabaseDocumentInternal database) {
    schema.create(database);
    indexManager.create(database);
    security.create();
    functionLibrary.create(database);
    sequenceLibrary.create(database);
    security.createClassTrigger();
    scheduler.create(database);

    // CREATE BASE VERTEX AND EDGE CLASSES
    schema.createClass("V");
    schema.createClass("E");
    loaded = true;
  }

  public OSchemaShared getSchema() {
    return schema;
  }

  public OSecurity getSecurity() {
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

  public OCommandCache getCommandCache() {
    return commandCache;
  }

  public OStatementCache getStatementCache() {
    return statementCache;
  }

  public OQueryStats getQueryStats(){
    return queryStats;
  }

}
