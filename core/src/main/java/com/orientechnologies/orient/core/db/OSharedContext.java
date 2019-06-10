package com.orientechnologies.orient.core.db;

import com.orientechnologies.common.listener.OListenerManger;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.cache.OCommandCache;
import com.orientechnologies.orient.core.db.viewmanager.ViewManager;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibraryImpl;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.metadata.security.OSecurityInternal;
import com.orientechnologies.orient.core.metadata.security.OSecurityShared;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibraryImpl;
import com.orientechnologies.orient.core.query.live.OLiveQueryHook;
import com.orientechnologies.orient.core.query.live.OLiveQueryHookV2;
import com.orientechnologies.orient.core.schedule.OSchedulerImpl;
import com.orientechnologies.orient.core.sql.executor.OQueryStats;
import com.orientechnologies.orient.core.sql.parser.OExecutionPlanCache;
import com.orientechnologies.orient.core.sql.parser.OStatementCache;
import com.orientechnologies.orient.core.storage.OStorage;

/**
 * Created by tglman on 15/06/16.
 */
public abstract class OSharedContext extends OListenerManger<OMetadataUpdateListener> {
  protected static final OProfiler PROFILER = Orient.instance().getProfiler();

  protected OrientDBInternal               orientDB;
  protected OStorage                       storage;
  protected OSchemaShared                  schema;
  protected OSecurityInternal              security;
  protected OIndexManagerAbstract          indexManager;
  protected OFunctionLibraryImpl           functionLibrary;
  protected OSchedulerImpl                 scheduler;
  protected OSequenceLibraryImpl           sequenceLibrary;
  protected OLiveQueryHook.OLiveQueryOps   liveQueryOps;
  protected OLiveQueryHookV2.OLiveQueryOps liveQueryOpsV2;
  protected OCommandCache                  commandCache;
  protected OStatementCache                statementCache;
  protected OExecutionPlanCache            executionPlanCache;
  protected OQueryStats                    queryStats;
  protected volatile boolean loaded = false;

  public OSharedContext() {
    super(true);
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

  public OCommandCache getCommandCache() {
    return commandCache;
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

  public abstract void load(ODatabaseDocumentInternal oDatabaseDocumentInternal);

  public abstract void reload(ODatabaseDocumentInternal database);

  public abstract void close();

  public OStorage getStorage() {
    return storage;
  }

  public OrientDBInternal getOrientDB() {
    return orientDB;
  }

  public void setStorage(OStorage storage) {
    this.storage = storage;
  }

  public ViewManager getViewManager() {
    throw new UnsupportedOperationException();
  }
}
