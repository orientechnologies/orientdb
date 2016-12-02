/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.cache.OCommandCacheHook;
import com.orientechnologies.orient.core.cache.OLocalRecordCache;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.record.OClassTrigger;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.index.OClassIndexManager;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.function.OFunctionTrigger;
import com.orientechnologies.orient.core.metadata.security.*;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceTrigger;
import com.orientechnologies.orient.core.query.live.OLiveQueryHook;
import com.orientechnologies.orient.core.schedule.OSchedulerTrigger;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import com.orientechnologies.orient.core.sql.executor.OTodoResultSet;
import com.orientechnologies.orient.core.storage.OStorage;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by tglman on 27/06/16.
 */
public class ODatabaseDocumentEmbedded extends ODatabaseDocumentAbstract implements OQueryLifecycleListener {

  private OrientDBConfig config;
  private OStorage       storage;

  AtomicLong nextRunningQuery = new AtomicLong(0);
  private Map<OTodoResultSet, OTodoResultSet> activeQueries = new IdentityHashMap<>();

  public ODatabaseDocumentEmbedded(final OStorage storage) {
    activateOnCurrentThread();

    try {
      status = STATUS.CLOSED;

      // OVERWRITE THE URL
      url = storage.getURL();
      this.storage = storage;
      this.componentsFactory = storage.getComponentsFactory();

      unmodifiableHooks = Collections.unmodifiableMap(hooks);

      localCache = new OLocalRecordCache();

      init();

      databaseOwner = this;
    } catch (Exception t) {
      ODatabaseRecordThreadLocal.INSTANCE.remove();

      throw OException.wrapException(new ODatabaseException("Error on opening database "), t);
    }

  }

  public <DB extends ODatabase> DB open(final String iUserName, final String iUserPassword) {
    throw new UnsupportedOperationException("Use OrientDBFactory");
  }

  public void internalOpen(final String iUserName, final String iUserPassword, OrientDBConfig config) {
    internalOpen(iUserName, iUserPassword, config, true);
  }

  private void internalOpen(final String iUserName, final String iUserPassword, OrientDBConfig config, boolean checkPassword) {
    activateOnCurrentThread();
    this.config = config;
    applyAttributes(config);
    applyListeners(config);
    try {

      if (user != null && !user.getName().equals(iUserName))
        initialized = false;

      status = STATUS.OPEN;

      initAtFirstOpen();

      final OSecurity security = metadata.getSecurity();

      if (user == null || user.getVersion() != security.getVersion() || !user.getName().equalsIgnoreCase(iUserName)) {
        final OUser usr;
        if (checkPassword) {
          usr = metadata.getSecurity().authenticate(iUserName, iUserPassword);
        } else {
          usr = metadata.getSecurity().getUser(iUserName);
        }
        if (usr != null)
          user = new OImmutableUser(security.getVersion(), usr);
        else
          user = null;

        checkSecurity(ORule.ResourceGeneric.DATABASE, ORole.PERMISSION_READ);
      }

      // WAKE UP LISTENERS
      callOnOpenListeners();

    } catch (OException e) {
      ODatabaseRecordThreadLocal.INSTANCE.remove();
      throw e;
    } catch (Exception e) {
      ODatabaseRecordThreadLocal.INSTANCE.remove();
      throw OException.wrapException(new ODatabaseException("Cannot open database url=" + getURL()), e);
    }
  }

  private void applyListeners(OrientDBConfig config) {
    if (config != null) {
      for (ODatabaseListener listener : config.getListeners()) {
        registerListener(listener);
      }
    }
  }

  /**
   * Opens a database using an authentication token received as an argument.
   *
   * @param iToken Authentication token
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  @Deprecated public <DB extends ODatabase> DB open(final OToken iToken) {
    throw new UnsupportedOperationException("Deprecated Method");
  }

  @Override public <DB extends ODatabase> DB create() {
    throw new UnsupportedOperationException("Deprecated Method");
  }

  /**
   * {@inheritDoc}
   *
   * @param config
   */
  public void internalCreate(OrientDBConfig config) {
    this.status = STATUS.OPEN;
    // THIS IF SHOULDN'T BE NEEDED, CREATE HAPPEN ONLY IN EMBEDDED
    applyAttributes(config);
    applyListeners(config);
    metadata = new OMetadataDefault(this);
    installHooksEmbedded();
    // CREATE THE DEFAULT SCHEMA WITH DEFAULT USER
    OSharedContext shared = getStorage().getResource(OSharedContext.class.getName(), new Callable<OSharedContext>() {
      @Override public OSharedContext call() throws Exception {
        OSharedContext shared = new OSharedContext(getStorage());
        return shared;
      }
    });
    metadata.init(shared);
    shared.create(this);

    registerHook(new OCommandCacheHook(this), ORecordHook.HOOK_POSITION.REGULAR);
    registerHook(new OSecurityTrackerHook(metadata.getSecurity(), this), ORecordHook.HOOK_POSITION.LAST);

    // WAKE UP DB LIFECYCLE LISTENER
    for (Iterator<ODatabaseLifecycleListener> it = Orient.instance().getDbLifecycleListeners(); it.hasNext(); )
      it.next().onCreate(getDatabaseOwner());

    // WAKE UP LISTENERS
    for (ODatabaseListener listener : browseListeners())
      try {
        listener.onCreate(this);
      } catch (Throwable ignore) {
      }

  }

  private void applyAttributes(OrientDBConfig config) {
    if (config != null) {
      for (Entry<ATTRIBUTES, Object> attrs : config.getAttributes().entrySet()) {
        this.set(attrs.getKey(), attrs.getValue());
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override public <DB extends ODatabase> DB create(String incrementalBackupPath) {
    throw new UnsupportedOperationException("use OrientDBFactory");
  }

  @Override public <DB extends ODatabase> DB create(final Map<OGlobalConfiguration, Object> iInitialSettings) {
    throw new UnsupportedOperationException("use OrientDBFactory");
  }

  /**
   * {@inheritDoc}
   */
  @Override public void drop() {
    throw new UnsupportedOperationException("use OrientDBFactory");
  }

  /**
   * Returns a copy of current database if it's open. The returned instance can be used by another thread without affecting current
   * instance. The database copy is not set in thread local.
   */
  public ODatabaseDocumentInternal copy() {
    ODatabaseDocumentEmbedded database = new ODatabaseDocumentEmbedded(storage);
    database.internalOpen(getUser().getName(), null, config, false);
    this.activateOnCurrentThread();
    return database;
  }

  @Override public boolean exists() {
    throw new UnsupportedOperationException("use OrientDBFactory");
  }

  @Override public void close() {
    checkIfActive();

    closeActiveQueries();

    localCache.shutdown();

    if (isClosed()) {
      status = STATUS.CLOSED;
      return;
    }

    try {
      commit(true);
    } catch (Exception e) {
      OLogManager.instance().error(this, "Exception during commit of active transaction", e);
    }

    if (status != STATUS.OPEN)
      return;

    callOnCloseListeners();

    if (currentIntent != null) {
      currentIntent.end(this);
      currentIntent = null;
    }
    sharedContext = null;
    status = STATUS.CLOSED;

    localCache.clear();

    if (storage != null)
      storage.close();

    ODatabaseRecordThreadLocal.INSTANCE.remove();
  }

  protected void closeActiveQueries() {
    while (activeQueries.size() > 0) {
      this.activeQueries.keySet().iterator().next().close();//the query automatically unregisters itself
    }
  }

  @Override public boolean isClosed() {
    return status == STATUS.CLOSED || storage.isClosed();
  }

  private void initAtFirstOpen() {
    if (initialized)
      return;

    ORecordSerializerFactory serializerFactory = ORecordSerializerFactory.instance();
    String serializeName = getStorage().getConfiguration().getRecordSerializer();
    if (serializeName == null)
      serializeName = ORecordSerializerSchemaAware2CSV.NAME;
    serializer = serializerFactory.getFormat(serializeName);
    if (serializer == null)
      throw new ODatabaseException("RecordSerializer with name '" + serializeName + "' not found ");
    if (getStorage().getConfiguration().getRecordSerializerVersion() > serializer.getMinSupportedVersion())
      throw new ODatabaseException("Persistent record serializer version is not support by the current implementation");

    localCache.startup();

    loadMetadata();

    if (metadata.getIndexManager().autoRecreateIndexesAfterCrash()) {
      metadata.getIndexManager().recreateIndexes();
    }

    installHooksEmbedded();
    registerHook(new OCommandCacheHook(this), ORecordHook.HOOK_POSITION.REGULAR);
    registerHook(new OSecurityTrackerHook(metadata.getSecurity(), this), ORecordHook.HOOK_POSITION.LAST);

    user = null;

    initialized = true;
  }

  protected void installHooksEmbedded() {
    hooks.clear();
    registerHook(new OClassTrigger(this), ORecordHook.HOOK_POSITION.FIRST);
    registerHook(new ORestrictedAccessHook(this), ORecordHook.HOOK_POSITION.FIRST);
    registerHook(new OUserTrigger(this), ORecordHook.HOOK_POSITION.EARLY);
    registerHook(new OFunctionTrigger(this), ORecordHook.HOOK_POSITION.REGULAR);
    registerHook(new OSequenceTrigger(this), ORecordHook.HOOK_POSITION.REGULAR);
    registerHook(new OClassIndexManager(this), ORecordHook.HOOK_POSITION.LAST);
    registerHook(new OSchedulerTrigger(this), ORecordHook.HOOK_POSITION.LAST);
    registerHook(new OLiveQueryHook(this), ORecordHook.HOOK_POSITION.LAST);
  }

  @Override public OStorage getStorage() {
    return storage;
  }

  @Override public void replaceStorage(OStorage iNewStorage) {
    storage = iNewStorage;
  }

  public void queryStarted(OTodoResultSet rs) {
    this.activeQueries.put(rs, rs);
  }

  public void queryClosed(OTodoResultSet rs) {
    this.activeQueries.remove(rs);
  }

}
