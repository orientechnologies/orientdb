package com.orientechnologies.orient.client.remote.db.document;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.client.remote.ORemoteClient;
import com.orientechnologies.orient.client.remote.OrientDBRemote;
import com.orientechnologies.orient.client.remote.metadata.schema.OSchemaRemote;
import com.orientechnologies.orient.client.remote.metadata.security.OSecurityRemote;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OSharedContext;
import com.orientechnologies.orient.core.db.OStringCache;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.viewmanager.ViewManager;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.index.OIndexManagerRemote;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibraryImpl;
import com.orientechnologies.orient.core.metadata.security.OSecurityInternal;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibraryImpl;
import com.orientechnologies.orient.core.schedule.OSchedulerImpl;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageInfo;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/** Created by tglman on 13/06/17. */
public class OSharedContextRemote extends OSharedContext {
  protected static final OProfiler PROFILER = Orient.instance().getProfiler();

  protected OrientDBRemote orientDB;
  protected ORemoteClient storage;
  protected OSchemaRemote schema;
  protected OSecurityRemote security;
  protected OIndexManagerRemote indexManager;
  protected OFunctionLibraryImpl functionLibrary;
  protected OSchedulerImpl scheduler;
  protected OSequenceLibraryImpl sequenceLibrary;
  protected volatile boolean loaded = false;
  protected Map<String, Object> resources;
  protected OStringCache stringCache;
  private final AtomicInteger sessionCount = new AtomicInteger(0);
  private volatile long lastCloseTime = System.currentTimeMillis();

  public OSharedContextRemote(ORemoteClient storage, OrientDBRemote orientDBRemote) {
    stringCache =
        new OStringCache(
            orientDBRemote
                .getContextConfiguration()
                .getValueAsInteger(OGlobalConfiguration.DB_STRING_CAHCE_SIZE));
    orientDB = orientDBRemote;
    this.storage = storage;
    schema = new OSchemaRemote();
    security = new OSecurityRemote();
    indexManager = new OIndexManagerRemote(storage);
    functionLibrary = new OFunctionLibraryImpl();
    scheduler = new OSchedulerImpl(orientDB);
    sequenceLibrary = new OSequenceLibraryImpl();
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
        sequenceLibrary.load(database);
        schema.onPostIndexManagement();
        loaded = true;
        ((ODatabaseDocumentRemote) database).initPush(new OPushListener(this));
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
    schema.close();
    security.close();
    indexManager.close();
    sequenceLibrary.close();
    loaded = false;
    getClient().shutdown();
  }

  @Override
  public synchronized void unload() {
    close();
  }

  public synchronized void reload(ODatabaseDocumentInternal database) {
    schema.reload(database);
    indexManager.reload(database);
    // The Immutable snapshot should be after index and schema that require and before everything
    // else that use it
    schema.forceSnapshot(database);
    security.load(database);
    scheduler.load(database);
    sequenceLibrary.load(database);
    functionLibrary.load(database);
  }

  public ORemoteClient getClient() {
    return storage;
  }

  @Override
  public boolean isLoaded() {
    return loaded;
  }

  public OSchemaRemote getSchema() {
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

  public OStorageInfo getStorage() {
    return storage;
  }

  public OrientDBInternal getOrientDB() {
    return orientDB;
  }

  public ViewManager getViewManager() {
    throw new UnsupportedOperationException();
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

  public synchronized void reInit(OStorage storage, ODatabaseDocumentInternal database) {
    throw new UnsupportedOperationException();
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
