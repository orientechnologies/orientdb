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
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.OScriptExecutor;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.record.OClassTrigger;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.index.OClassIndexManager;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.function.OFunctionTrigger;
import com.orientechnologies.orient.core.metadata.security.*;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceTrigger;
import com.orientechnologies.orient.core.query.live.OLiveQueryHook;
import com.orientechnologies.orient.core.query.live.OLiveQueryHookV2;
import com.orientechnologies.orient.core.query.live.OLiveQueryListenerV2;
import com.orientechnologies.orient.core.query.live.OLiveQueryMonitorEmbedded;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.schedule.OSchedulerTrigger;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.executor.LiveQueryListenerImpl;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OInternalExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.parser.OLocalResultSet;
import com.orientechnologies.orient.core.sql.parser.OLocalResultSetLifecycleDecorator;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OMicroTransaction;

import java.util.Collections;
import java.util.HashMap;
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
  private Map<String, OLocalResultSetLifecycleDecorator> activeQueries = new HashMap<>();

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
    throw new UnsupportedOperationException("Use OrientDB");
  }

  public void init(OrientDBConfig config) {
    activateOnCurrentThread();
    this.config = config;
    applyAttributes(config);
    applyListeners(config);
    try {

      status = STATUS.OPEN;
      if (initialized)
        return;

      ORecordSerializerFactory serializerFactory = ORecordSerializerFactory.instance();
      String serializeName = getStorage().getConfiguration().getRecordSerializer();
      if (serializeName == null)
        throw new ODatabaseException("Impossible to open database from version before 2.x use export import instead");
      serializer = serializerFactory.getFormat(serializeName);
      if (serializer == null)
        throw new ODatabaseException("RecordSerializer with name '" + serializeName + "' not found ");
      if (getStorage().getConfiguration().getRecordSerializerVersion() > serializer.getMinSupportedVersion())
        throw new ODatabaseException("Persistent record serializer version is not support by the current implementation");

      localCache.startup();

      loadMetadata();

      installHooksEmbedded();
      registerHook(new OCommandCacheHook(this), ORecordHook.HOOK_POSITION.REGULAR);
      registerHook(new OSecurityTrackerHook(metadata.getSecurity(), this), ORecordHook.HOOK_POSITION.LAST);

      user = null;

      initialized = true;
    } catch (OException e) {
      ODatabaseRecordThreadLocal.INSTANCE.remove();
      throw e;
    } catch (Exception e) {
      ODatabaseRecordThreadLocal.INSTANCE.remove();
      throw OException.wrapException(new ODatabaseException("Cannot open database url=" + getURL()), e);
    }

  }

  public void internalOpen(final String iUserName, final String iUserPassword) {
    internalOpen(iUserName, iUserPassword, true);
  }

  public void internalOpen(final String iUserName, final String iUserPassword, boolean checkPassword) {
    try {
      OSecurity security = metadata.getSecurity();

      if (user == null || user.getVersion() != security.getVersion() || !user.getName().equalsIgnoreCase(iUserName)) {
        final OUser usr;

        if (checkPassword) {
          usr = security.authenticate(iUserName, iUserPassword);
        } else {
          usr = security.getUser(iUserName);
        }
        if (usr != null)
          user = new OImmutableUser(security.getVersion(), usr);
        else
          user = null;

        checkSecurity(ORule.ResourceGeneric.DATABASE, ORole.PERMISSION_READ);
      }

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
   *
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  @Deprecated
  public <DB extends ODatabase> DB open(final OToken iToken) {
    throw new UnsupportedOperationException("Deprecated Method");
  }

  @Override
  public <DB extends ODatabase> DB create() {
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
    createMetadata();

    registerHook(new OCommandCacheHook(this), ORecordHook.HOOK_POSITION.REGULAR);
    registerHook(new OSecurityTrackerHook(metadata.getSecurity(), this), ORecordHook.HOOK_POSITION.LAST);
  }

  public void callOnCreateListeners() {
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

  protected void createMetadata() {
    // CREATE THE DEFAULT SCHEMA WITH DEFAULT USER
    OSharedContext shared = getStorage().getResource(OSharedContext.class.getName(), new Callable<OSharedContext>() {
      @Override
      public OSharedContext call() throws Exception {
        OSharedContext shared = new OSharedContextEmbedded(getStorage());
        return shared;
      }
    });
    metadata.init(shared);
    ((OSharedContextEmbedded) shared).create(this);
  }

  @Override
  protected void loadMetadata() {
    metadata = new OMetadataDefault(this);
    sharedContext = getStorage().getResource(OSharedContext.class.getName(), new Callable<OSharedContext>() {
      @Override
      public OSharedContext call() throws Exception {
        OSharedContext shared = new OSharedContextEmbedded(getStorage());
        return shared;
      }
    });
    metadata.init(sharedContext);
    sharedContext.load(this);
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
  @Override
  public <DB extends ODatabase> DB create(String incrementalBackupPath) {
    throw new UnsupportedOperationException("use OrientDB");
  }

  @Override
  public <DB extends ODatabase> DB create(final Map<OGlobalConfiguration, Object> iInitialSettings) {
    throw new UnsupportedOperationException("use OrientDB");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void drop() {
    throw new UnsupportedOperationException("use OrientDB");
  }

  /**
   * Returns a copy of current database if it's open. The returned instance can be used by another thread without affecting current
   * instance. The database copy is not set in thread local.
   */
  public ODatabaseDocumentInternal copy() {
    ODatabaseDocumentEmbedded database = new ODatabaseDocumentEmbedded(storage);
    database.init(config);
    database.internalOpen(getUser().getName(), null, false);
    database.callOnOpenListeners();
    this.activateOnCurrentThread();
    return database;
  }

  @Override
  public boolean exists() {
    throw new UnsupportedOperationException("use OrientDB");
  }

  @Override
  public void close() {
    checkIfActive();

    closeActiveQueries();

    localCache.shutdown();

    if (isClosed()) {
      status = STATUS.CLOSED;
      return;
    }

    try {
      rollback(true);
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
      this.activeQueries.values().iterator().next().close();//the query automatically unregisters itself
    }
  }

  @Override
  public boolean isClosed() {
    return status == STATUS.CLOSED || storage.isClosed();
  }

  public void rebuildIndexes() {
    if (metadata.getIndexManager().autoRecreateIndexesAfterCrash()) {
      metadata.getIndexManager().recreateIndexes();
    }
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
    registerHook(new OLiveQueryHookV2(this), ORecordHook.HOOK_POSITION.LAST);
  }

  @Override
  public OStorage getStorage() {
    return storage;
  }

  @Override
  public void replaceStorage(OStorage iNewStorage) {
    storage = iNewStorage;
  }

  public void queryStarted(OLocalResultSetLifecycleDecorator rs) {
    this.activeQueries.put(rs.getQueryId(), rs);
  }

  public void queryClosed(OLocalResultSetLifecycleDecorator rs) {
    this.activeQueries.remove(rs.getQueryId());
  }

  @Override
  public OResultSet query(String query, Object[] args) {
    OStatement statement = OSQLEngine.parse(query, this);
    if (!statement.isIdempotent()) {
      throw new OCommandExecutionException("Cannot execute query on non idempotent statement: " + query);
    }
    OResultSet original = statement.execute(this, args);
    OLocalResultSetLifecycleDecorator result = new OLocalResultSetLifecycleDecorator(original);
    this.queryStarted(result);
    result.addLifecycleListener(this);
    return result;
  }

  @Override
  public OResultSet query(String query, Map args) {
    OStatement statement = OSQLEngine.parse(query, this);
    if (!statement.isIdempotent()) {
      throw new OCommandExecutionException("Cannot execute query on non idempotent statement: " + query);
    }
    OResultSet original = statement.execute(this, args);
    OLocalResultSetLifecycleDecorator result = new OLocalResultSetLifecycleDecorator(original);
    this.queryStarted(result);
    result.addLifecycleListener(this);
    return result;
  }

  @Override
  public OResultSet command(String query, Object[] args) {
    OStatement statement = OSQLEngine.parse(query, this);
    OResultSet original = statement.execute(this, args);
    OLocalResultSetLifecycleDecorator result = new OLocalResultSetLifecycleDecorator(original);
    this.queryStarted(result);
    result.addLifecycleListener(this);
    return result;
  }

  @Override
  public OResultSet command(String query, Map args) {
    OStatement statement = OSQLEngine.parse(query, this);
    OResultSet original = statement.execute(this, args);
    OLocalResultSetLifecycleDecorator result = new OLocalResultSetLifecycleDecorator(original);
    this.queryStarted(result);
    result.addLifecycleListener(this);
    return result;
  }

  @Override
  public OResultSet execute(String language, String script, Object... args) {
    OScriptExecutor executor = OCommandManager.instance().getScriptExecutor(language);
    OResultSet original = executor.execute(this, script, args);
    OLocalResultSetLifecycleDecorator result = new OLocalResultSetLifecycleDecorator(original);
    this.queryStarted(result);
    result.addLifecycleListener(this);
    return result;
  }

  @Override
  public OResultSet execute(String language, String script, Map<String, ?> args) {
    OScriptExecutor executor = OCommandManager.instance().getScriptExecutor(language);
    OResultSet original = executor.execute(this, script, args);
    OLocalResultSetLifecycleDecorator result = new OLocalResultSetLifecycleDecorator(original);
    this.queryStarted(result);
    result.addLifecycleListener(this);
    return result;
  }

  public OLocalResultSetLifecycleDecorator query(OExecutionPlan plan, Map<Object, Object> params) {
    OBasicCommandContext ctx = new OBasicCommandContext();
    ctx.setDatabase(this);
    ctx.setInputParameters(params);

    OLocalResultSet result = new OLocalResultSet((OInternalExecutionPlan) plan);
    OLocalResultSetLifecycleDecorator decorator = new OLocalResultSetLifecycleDecorator(result);
    this.queryStarted(decorator);
    decorator.addLifecycleListener(this);

    return decorator;
  }

  public OLocalResultSetLifecycleDecorator getActiveQuery(String id) {
    return activeQueries.get(id);
  }

  public OrientDBConfig getConfig() {
    return config;
  }

  @Override
  public OLiveQueryMonitor live(String query, OLiveQueryResultListener listener, Object... args) {
    OLiveQueryListenerV2 queryListener = new LiveQueryListenerImpl(listener, query, this, args);
    ODatabaseDocumentInternal dbCopy = this.copy();
    this.activateOnCurrentThread();
    OLiveQueryMonitor monitor = new OLiveQueryMonitorEmbedded(queryListener.getToken(), dbCopy);
    return monitor;
  }

  @Override
  public OLiveQueryMonitor live(String query, OLiveQueryResultListener listener, Map<String, ?> args) {
    OLiveQueryListenerV2 queryListener = new LiveQueryListenerImpl(listener, query, this, (Map) args);
    ODatabaseDocumentInternal dbCopy = this.copy();
    this.activateOnCurrentThread();
    OLiveQueryMonitor monitor = new OLiveQueryMonitorEmbedded(queryListener.getToken(), dbCopy);
    return monitor;
  }

  @Override
  public void recycle(final ORecord record) {
    throw new UnsupportedOperationException();
  }

  protected OMicroTransaction beginMicroTransaction() {
    final OAbstractPaginatedStorage abstractPaginatedStorage = (OAbstractPaginatedStorage) getStorage().getUnderlying();

    if (microTransaction == null)
      microTransaction = new OMicroTransaction(abstractPaginatedStorage, this);

    microTransaction.begin();
    return microTransaction;
  }

}
