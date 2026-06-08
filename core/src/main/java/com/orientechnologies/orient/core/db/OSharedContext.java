package com.orientechnologies.orient.core.db;

import com.orientechnologies.common.listener.OListenerManger;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibraryImpl;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.core.metadata.security.OSecurityInternal;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibraryImpl;
import com.orientechnologies.orient.core.schedule.OSchedulerImpl;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageInfo;
import java.util.concurrent.Callable;

/** Created by tglman on 15/06/16. */
public abstract class OSharedContext extends OListenerManger<OMetadataUpdateListener> {
  protected static final OProfiler PROFILER = Orient.instance().getProfiler();

  public OSharedContext() {
    super(true);
  }

  public abstract OSchemaShared getSchema();

  public abstract OSecurityInternal getSecurity();

  public abstract OIndexManagerAbstract getIndexManager();

  public abstract OFunctionLibraryImpl getFunctionLibrary();

  public abstract OSchedulerImpl getScheduler();

  public abstract OSequenceLibraryImpl getSequenceLibrary();

  public abstract void load(ODatabaseDocumentInternal database);

  public abstract void reload(ODatabaseDocumentInternal database);

  public abstract void unload();

  public abstract void close();

  public abstract OStorageInfo getStorage();

  public abstract OrientDBInternal getOrientDB();

  public abstract <T> T getResource(final String name, final Callable<T> factory);

  public abstract void reInit(OStorage storage, ODatabaseDocumentInternal database);

  public abstract OStringCache getStringCache();

  public abstract void startSession();

  public abstract void endSession();

  public abstract int getSessionCount();

  public abstract long getLastCloseTime();

  public abstract boolean isLoaded();
}
