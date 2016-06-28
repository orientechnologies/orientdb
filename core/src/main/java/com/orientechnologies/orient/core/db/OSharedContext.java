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
    commandCache = new OCommandCacheSoftRefs(storage.getName());
    statementCache = new OStatementCache(OGlobalConfiguration.STATEMENT_CACHE_SIZE.getValueAsInteger());
  }

  public synchronized void load(OStorage storage) {
    final long timer = PROFILER.startChrono();

    try {
      if (!loaded) {
        schema.load();
        security.load();
        indexManager.load();
        if (!(storage instanceof OStorageProxy)) {
          functionLibrary.load();
          scheduler.load();
        }
        sequenceLibrary.load();
        schema.onPostIndexManagement();
        loaded = true;
      }
    } finally {
      PROFILER
          .stopChrono(PROFILER.getDatabaseMetric(storage.getName(), "metadata.load"), "Loading of database metadata", timer,
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

  public synchronized void reload() {
    schema.reload();
    indexManager.reload();
    security.load();
    functionLibrary.load();
    sequenceLibrary.load();
    commandCache.clear();
    scheduler.load();
  }

  public synchronized void create() {
    schema.create();
    indexManager.create();
    security.create();
    functionLibrary.create();
    sequenceLibrary.create();
    security.createClassTrigger();
    scheduler.create();

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

}
