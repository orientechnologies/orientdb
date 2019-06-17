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

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.cache.OCommandCacheHook;
import com.orientechnologies.orient.core.cache.OLocalRecordCache;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.OScriptExecutor;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OHookReplacedRecordThreadLocal;
import com.orientechnologies.orient.core.db.OLiveQueryMonitor;
import com.orientechnologies.orient.core.db.OLiveQueryResultListener;
import com.orientechnologies.orient.core.db.OSharedContext;
import com.orientechnologies.orient.core.db.OSharedContextEmbedded;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.record.OClassTrigger;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.*;
import com.orientechnologies.orient.core.fetch.OFetchHelper;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OClassIndexManager;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;
import com.orientechnologies.orient.core.metadata.schema.OView;
import com.orientechnologies.orient.core.metadata.security.*;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceAction;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibraryProxy;
import com.orientechnologies.orient.core.query.live.OLiveQueryHook;
import com.orientechnologies.orient.core.query.live.OLiveQueryHookV2;
import com.orientechnologies.orient.core.query.live.OLiveQueryListenerV2;
import com.orientechnologies.orient.core.query.live.OLiveQueryMonitorEmbedded;
import com.orientechnologies.orient.core.record.*;
import com.orientechnologies.orient.core.record.impl.*;
import com.orientechnologies.orient.core.schedule.OScheduledEvent;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.executor.LiveQueryListenerImpl;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OInternalExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OInternalResultSet;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.parser.OLocalResultSet;
import com.orientechnologies.orient.core.sql.parser.OLocalResultSetLifecycleDecorator;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import com.orientechnologies.orient.core.storage.OBasicTransaction;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.cluster.OOfflineClusterException;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OMicroTransaction;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordSerializationContext;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created by tglman on 27/06/16.
 */
public class ODatabaseDocumentEmbedded extends ODatabaseDocumentAbstract implements OQueryLifecycleListener {

  private OrientDBConfig config;
  private OStorage       storage;

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
      ODatabaseRecordThreadLocal.instance().remove();

      throw OException.wrapException(new ODatabaseException("Error on opening database "), t);
    }

  }

  public <DB extends ODatabase> DB open(final String iUserName, final String iUserPassword) {
    throw new UnsupportedOperationException("Use OrientDB");
  }

  public void init(OrientDBConfig config, OSharedContext sharedContext) {
    this.sharedContext = sharedContext;
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

      loadMetadata(this.sharedContext);

      installHooksEmbedded();
      if (this.getMetadata().getCommandCache().isEnabled())
        registerHook(new OCommandCacheHook(this), ORecordHook.HOOK_POSITION.REGULAR);

      user = null;

      initialized = true;
    } catch (OException e) {
      ODatabaseRecordThreadLocal.instance().remove();
      throw e;
    } catch (Exception e) {
      ODatabaseRecordThreadLocal.instance().remove();
      throw OException.wrapException(new ODatabaseException("Cannot open database url=" + getURL()), e);
    }

  }

  public void internalOpen(final String iUserName, final String iUserPassword) {
    internalOpen(iUserName, iUserPassword, true);
  }

  public void internalOpen(final String iUserName, final String iUserPassword, boolean checkPassword) {
    try {
      OSecurityInternal security = sharedContext.getSecurity();

      if (user == null || user.getVersion() != security.getVersion(this) || !user.getName().equalsIgnoreCase(iUserName)) {
        final OUser usr;

        if (checkPassword) {
          usr = security.authenticate(this, iUserName, iUserPassword);
        } else {
          usr = security.getUser(this, iUserName);
        }
        if (usr != null)
          user = new OImmutableUser(security.getVersion(this), usr);
        else
          user = null;

        checkSecurity(ORule.ResourceGeneric.DATABASE, ORole.PERMISSION_READ);
      }

    } catch (OException e) {
      ODatabaseRecordThreadLocal.instance().remove();
      throw e;
    } catch (Exception e) {
      ODatabaseRecordThreadLocal.instance().remove();
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
   */
  public void internalCreate(OrientDBConfig config, OSharedContext ctx) {
    this.sharedContext = ctx;
    this.status = STATUS.OPEN;
    // THIS IF SHOULDN'T BE NEEDED, CREATE HAPPEN ONLY IN EMBEDDED
    applyAttributes(config);
    applyListeners(config);
    metadata = new OMetadataDefault(this);
    installHooksEmbedded();
    createMetadata(ctx);

    if (this.getMetadata().getCommandCache().isEnabled())
      registerHook(new OCommandCacheHook(this), ORecordHook.HOOK_POSITION.REGULAR);
  }

  public void callOnCreateListeners() {
    // WAKE UP DB LIFECYCLE LISTENER
    for (Iterator<ODatabaseLifecycleListener> it = Orient.instance().getDbLifecycleListeners(); it.hasNext(); )
      it.next().onCreate(getDatabaseOwner());

    // WAKE UP LISTENERS
    for (ODatabaseListener listener : browseListeners())
      try {
        listener.onCreate(this);
      } catch (Exception ignore) {
      }
  }

  protected void createMetadata(OSharedContext shared) {
    metadata.init(shared);
    ((OSharedContextEmbedded) shared).create(this);
  }

  @Override
  protected void loadMetadata() {
    loadMetadata(this.sharedContext);
  }

  @Override
  protected void loadMetadata(OSharedContext shared) {
    metadata = new OMetadataDefault(this);
    sharedContext = shared;

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

  @Override
  public <DB extends ODatabase> DB set(final ATTRIBUTES iAttribute, final Object iValue) {
    checkIfActive();

    if (iAttribute == null)
      throw new IllegalArgumentException("attribute is null");

    final String stringValue = OIOUtils.getStringContent(iValue != null ? iValue.toString() : null);
    final OStorage storage = getStorage();
    switch (iAttribute) {
    case STATUS:
      if (stringValue == null)
        throw new IllegalArgumentException("DB status can't be null");
      setStatus(STATUS.valueOf(stringValue.toUpperCase(Locale.ENGLISH)));
      break;

    case DEFAULTCLUSTERID:
      if (iValue != null) {
        if (iValue instanceof Number)
          storage.setDefaultClusterId(((Number) iValue).intValue());
        else
          storage.setDefaultClusterId(storage.getClusterIdByName(iValue.toString()));
      }
      break;

    case TYPE:
      throw new IllegalArgumentException("Database type cannot be changed at run-time");

    case DATEFORMAT:
      if (stringValue == null)
        throw new IllegalArgumentException("date format is null");

      // CHECK FORMAT
      new SimpleDateFormat(stringValue).format(new Date());

      storage.setDateFormat(stringValue);
      break;

    case DATETIMEFORMAT:
      if (stringValue == null)
        throw new IllegalArgumentException("date format is null");

      // CHECK FORMAT
      new SimpleDateFormat(stringValue).format(new Date());

      storage.setDateTimeFormat(stringValue);
      break;

    case TIMEZONE:
      if (stringValue == null)
        throw new IllegalArgumentException("Timezone can't be null");

      // for backward compatibility, until 2.1.13 OrientDB accepted timezones in lowercase as well
      TimeZone timeZoneValue = TimeZone.getTimeZone(stringValue.toUpperCase(Locale.ENGLISH));
      if (timeZoneValue.equals(TimeZone.getTimeZone("GMT"))) {
        timeZoneValue = TimeZone.getTimeZone(stringValue);
      }

      storage.setTimeZone(timeZoneValue);
      break;

    case LOCALECOUNTRY:
      storage.setLocaleCountry(stringValue);
      break;

    case LOCALELANGUAGE:
      storage.setLocaleLanguage(stringValue);
      break;

    case CHARSET:
      storage.setCharset(stringValue);
      break;

    case CUSTOM:
      int indx = stringValue != null ? stringValue.indexOf('=') : -1;
      if (indx < 0) {
        if ("clear".equalsIgnoreCase(stringValue)) {
          clearCustomInternal();
        } else
          throw new IllegalArgumentException("Syntax error: expected <name> = <value> or clear, instead found: " + iValue);
      } else {
        String customName = stringValue.substring(0, indx).trim();
        String customValue = stringValue.substring(indx + 1).trim();
        if (customValue.isEmpty())
          removeCustomInternal(customName);
        else
          setCustomInternal(customName, customValue);
      }
      break;

    case CLUSTERSELECTION:
      storage.setClusterSelection(stringValue);
      break;

    case MINIMUMCLUSTERS:
      if (iValue != null) {
        if (iValue instanceof Number)
          storage.setMinimumClusters(((Number) iValue).intValue());
        else
          storage.setMinimumClusters(Integer.parseInt(stringValue));
      } else
        // DEFAULT = 1
        storage.setMinimumClusters(1);

      break;

    case CONFLICTSTRATEGY:
      storage.setConflictStrategy(Orient.instance().getRecordConflictStrategy().getStrategy(stringValue));
      break;

    case VALIDATION:
      storage.setValidation(Boolean.parseBoolean(stringValue));
      break;

    default:
      throw new IllegalArgumentException("Option '" + iAttribute + "' not supported on alter database");

    }

    return (DB) this;
  }

  private void clearCustomInternal() {
    getStorage().clearProperties();
  }

  private void removeCustomInternal(final String iName) {
    setCustomInternal(iName, null);
  }

  private void setCustomInternal(final String iName, final String iValue) {
    final OStorage storage = getStorage();
    if (iValue == null || "null".equalsIgnoreCase(iValue))
      // REMOVE
      storage.removeProperty(iName);
    else
      // SET
      storage.setProperty(iName, iValue);
  }

  public <DB extends ODatabase> DB setCustom(final String name, final Object iValue) {
    checkIfActive();

    if ("clear".equalsIgnoreCase(name) && iValue == null) {
      clearCustomInternal();
    } else {
      String customName = name;
      String customValue = iValue == null ? null : "" + iValue;
      if (customName == null || customValue.isEmpty())
        removeCustomInternal(customName);
      else
        setCustomInternal(customName, customValue);
    }

    return (DB) this;
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
    ODatabaseDocumentEmbedded database = new ODatabaseDocumentEmbedded(getSharedContext().getStorage());
    database.init(config, this.sharedContext);
    String user;
    if (getUser() != null) {
      user = getUser().getName();
    } else {
      user = null;
    }
    database.internalOpen(user, null, false);
    database.callOnOpenListeners();
    this.activateOnCurrentThread();
    return database;
  }

  @Override
  public boolean exists() {
    throw new UnsupportedOperationException("use OrientDB");
  }

  @Override
  public boolean isClosed() {
    return status == STATUS.CLOSED || getStorage().isClosed();
  }

  public void rebuildIndexes() {
    if (metadata.getIndexManager().autoRecreateIndexesAfterCrash()) {
      metadata.getIndexManager().recreateIndexes();
    }
  }

  protected void installHooksEmbedded() {
    hooks.clear();
  }

  @Override
  public OStorage getStorage() {
    return storage;
  }

  @Override
  public void replaceStorage(OStorage iNewStorage) {
    this.getSharedContext().setStorage(iNewStorage);
    storage = iNewStorage;
  }

  @Override
  public OResultSet query(String query, Object[] args) {
    checkOpenness();
    checkIfActive();

    OStatement statement = OSQLEngine.parse(query, this);
    if (!statement.isIdempotent()) {
      throw new OCommandExecutionException("Cannot execute query on non idempotent statement: " + query);
    }
    OResultSet original = statement.execute(this, args, true);
    OLocalResultSetLifecycleDecorator result = new OLocalResultSetLifecycleDecorator(original);
    this.queryStarted(result.getQueryId(), result);
    result.addLifecycleListener(this);
    return result;
  }

  @Override
  public OResultSet query(String query, Map args) {
    checkOpenness();
    checkIfActive();

    OStatement statement = OSQLEngine.parse(query, this);
    if (!statement.isIdempotent()) {
      throw new OCommandExecutionException("Cannot execute query on non idempotent statement: " + query);
    }
    OResultSet original = statement.execute(this, args, true);
    OLocalResultSetLifecycleDecorator result = new OLocalResultSetLifecycleDecorator(original);
    this.queryStarted(result.getQueryId(), result);
    result.addLifecycleListener(this);
    return result;
  }

  @Override
  public OResultSet command(String query, Object[] args) {
    checkOpenness();
    checkIfActive();

    OStatement statement = OSQLEngine.parse(query, this);
    OResultSet original = statement.execute(this, args, true);
    OLocalResultSetLifecycleDecorator result;
    if (!statement.isIdempotent()) {
      //fetch all, close and detach
      OInternalResultSet prefetched = new OInternalResultSet();
      original.forEachRemaining(x -> prefetched.add(x));
      original.close();
      result = new OLocalResultSetLifecycleDecorator(prefetched);
    } else {
      //stream, keep open and attach to the current DB
      result = new OLocalResultSetLifecycleDecorator(original);
      this.queryStarted(result.getQueryId(), result);
      result.addLifecycleListener(this);
    }
    return result;

  }

  @Override
  public OResultSet command(String query, Map args) {
    checkOpenness();
    checkIfActive();

    OStatement statement = OSQLEngine.parse(query, this);
    OResultSet original = statement.execute(this, args, true);
    OLocalResultSetLifecycleDecorator result;
    if (!statement.isIdempotent()) {
      //fetch all, close and detach
      OInternalResultSet prefetched = new OInternalResultSet();
      original.forEachRemaining(x -> prefetched.add(x));
      original.close();
      result = new OLocalResultSetLifecycleDecorator(prefetched);
    } else {
      //stream, keep open and attach to the current DB
      result = new OLocalResultSetLifecycleDecorator(original);
      this.queryStarted(result.getQueryId(), result);
      result.addLifecycleListener(this);
    }
    return result;
  }

  @Override
  public OResultSet execute(String language, String script, Object... args) {
    checkOpenness();
    checkIfActive();

    OScriptExecutor executor = OCommandManager.instance().getScriptExecutor(language);

    ((OAbstractPaginatedStorage) this.storage.getUnderlying()).pauseConfigurationUpdateNotifications();
    OResultSet original;
    try {
      original = executor.execute(this, script, args);
    } finally {
      ((OAbstractPaginatedStorage) this.storage.getUnderlying()).fireConfigurationUpdateNotifications();
    }
    OLocalResultSetLifecycleDecorator result = new OLocalResultSetLifecycleDecorator(original);
    this.queryStarted(result.getQueryId(), result);
    result.addLifecycleListener(this);
    return result;
  }

  @Override
  public OResultSet execute(String language, String script, Map<String, ?> args) {
    checkOpenness();
    checkIfActive();

    OScriptExecutor executor = OCommandManager.instance().getScriptExecutor(language);
    OResultSet original;

    ((OAbstractPaginatedStorage) this.storage.getUnderlying()).pauseConfigurationUpdateNotifications();
    try {
      original = executor.execute(this, script, args);
    } finally {
      ((OAbstractPaginatedStorage) this.storage.getUnderlying()).fireConfigurationUpdateNotifications();
    }

    OLocalResultSetLifecycleDecorator result = new OLocalResultSetLifecycleDecorator(original);
    this.queryStarted(result.getQueryId(), result);
    result.addLifecycleListener(this);
    return result;
  }

  public OLocalResultSetLifecycleDecorator query(OExecutionPlan plan, Map<Object, Object> params) {
    checkOpenness();
    checkIfActive();

    OBasicCommandContext ctx = new OBasicCommandContext();
    ctx.setDatabase(this);
    ctx.setInputParameters(params);

    OLocalResultSet result = new OLocalResultSet((OInternalExecutionPlan) plan);
    OLocalResultSetLifecycleDecorator decorator = new OLocalResultSetLifecycleDecorator(result);
    this.queryStarted(decorator.getQueryId(), decorator);
    decorator.addLifecycleListener(this);

    return decorator;
  }

  public OrientDBConfig getConfig() {
    return config;
  }

  @Override
  public OLiveQueryMonitor live(String query, OLiveQueryResultListener listener, Object... args) {
    checkOpenness();
    checkIfActive();

    OLiveQueryListenerV2 queryListener = new LiveQueryListenerImpl(listener, query, this, args);
    ODatabaseDocumentInternal dbCopy = this.copy();
    this.activateOnCurrentThread();
    OLiveQueryMonitor monitor = new OLiveQueryMonitorEmbedded(queryListener.getToken(), dbCopy);
    return monitor;
  }

  @Override
  public OLiveQueryMonitor live(String query, OLiveQueryResultListener listener, Map<String, ?> args) {
    checkOpenness();
    checkIfActive();

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
    microTransaction.setNoTxLocks(((OTransactionAbstract) getTransaction()).getInternalLocks());
    return microTransaction;
  }

  @Override
  public int addBlobCluster(final String iClusterName, final Object... iParameters) {
    int id;
    if (!existsCluster(iClusterName)) {
      id = addCluster(iClusterName, iParameters);
    } else {
      id = getClusterIdByName(iClusterName);
    }
    getMetadata().getSchema().addBlobCluster(id);
    return id;
  }

  /**
   * This method is internal, it can be subject to signature change or be removed, do not use.
   *
   * @Internal
   */
  public void executeDeleteRecord(OIdentifiable record, final int iVersion, final boolean iRequired, final OPERATION_MODE iMode,
      boolean prohibitTombstones) {
    checkOpenness();
    checkIfActive();

    final ORecordId rid = (ORecordId) record.getIdentity();

    if (rid == null)
      throw new ODatabaseException(
          "Cannot delete record because it has no identity. Probably was created from scratch or contains projections of fields rather than a full record");

    if (!rid.isValid())
      return;

    record = record.getRecord();
    if (record == null)
      return;

    final OMicroTransaction microTx = beginMicroTransaction();
    try {
      microTx.deleteRecord(record.getRecord(), iMode);
    } catch (Exception e) {
      endMicroTransaction(false);
      throw e;
    }
    endMicroTransaction(true);
    return;
  }

  private void endMicroTransaction(boolean success) {
    assert microTransaction != null;

    try {
      if (success) {
        try {
          microTransaction.commit();
          OLiveQueryHook.notifyForTxChanges(this);
          OLiveQueryHookV2.notifyForTxChanges(this);
        } catch (Exception e) {
          microTransaction.rollbackAfterFailedCommit();
          OLiveQueryHook.removePendingDatabaseOps(this);
          OLiveQueryHookV2.removePendingDatabaseOps(this);
          throw e;
        }
      } else {
        microTransaction.rollback();
        OLiveQueryHook.removePendingDatabaseOps(this);
        OLiveQueryHookV2.removePendingDatabaseOps(this);
      }
    } finally {
      if (!microTransaction.isActive())
        microTransaction = null;
    }
  }

  @Override
  public OIdentifiable beforeCreateOperations(OIdentifiable id, String iClusterName) {
    checkClusterSecurity(ORole.PERMISSION_CREATE, id, iClusterName);

    ORecordHook.RESULT triggerChanged = null;
    boolean changed = false;
    if (id instanceof ODocument) {
      ODocument doc = (ODocument) id;
      OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(this, doc);
      if (clazz != null) {
        if (clazz.isScheduler()) {
          getSharedContext().getScheduler().initScheduleRecord(doc);
          changed = true;
        }
        if (clazz.isOuser()) {
          changed = OUser.encodePassword(doc);
        }
        if (clazz.isTriggered()) {
          triggerChanged = OClassTrigger.onRecordBeforeCreate(doc, this);
        }
        if (clazz.isRestricted()) {
          changed = ORestrictedAccessHook.onRecordBeforeCreate(doc, this);
        }
      }
    }

    ORecordHook.RESULT res = callbackHooks(ORecordHook.TYPE.BEFORE_CREATE, id);
    if (changed || res == ORecordHook.RESULT.RECORD_CHANGED || triggerChanged == ORecordHook.RESULT.RECORD_CHANGED) {
      if (id instanceof ODocument) {
        ((ODocument) id).validate();
      }
      return id;
    } else if (res == ORecordHook.RESULT.RECORD_REPLACED || triggerChanged == ORecordHook.RESULT.RECORD_REPLACED) {
      ORecord replaced = OHookReplacedRecordThreadLocal.INSTANCE.get();
      if (replaced instanceof ODocument) {
        ((ODocument) replaced).validate();
      }
      return replaced;
    }
    return null;
  }

  @Override
  public OIdentifiable beforeUpdateOperations(OIdentifiable id, String iClusterName) {
    checkClusterSecurity(ORole.PERMISSION_UPDATE, id, iClusterName);

    ORecordHook.RESULT triggerChanged = null;
    boolean changed = false;
    if (id instanceof ODocument) {
      ODocument doc = (ODocument) id;
      OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(this, doc);
      if (clazz != null) {
        if (clazz.isScheduler()) {
          getSharedContext().getScheduler().handleUpdateSchedule(doc);
          changed = true;
        }
        if (clazz.isOuser()) {
          changed = OUser.encodePassword(doc);
        }
        if (clazz.isTriggered()) {
          triggerChanged = OClassTrigger.onRecordBeforeUpdate(doc, this);
        }
        if (clazz.isRestricted()) {
          if (!ORestrictedAccessHook.isAllowed(this, doc, ORestrictedOperation.ALLOW_UPDATE, true))
            throw new OSecurityException("Cannot update record " + doc.getIdentity() + ": the resource has restricted access");
        }
      }
    }
    ORecordHook.RESULT res = callbackHooks(ORecordHook.TYPE.BEFORE_UPDATE, id);
    if (res == ORecordHook.RESULT.RECORD_CHANGED || triggerChanged == ORecordHook.RESULT.RECORD_CHANGED) {
      if (id instanceof ODocument) {
        ((ODocument) id).validate();
      }
      return id;
    } else if (res == ORecordHook.RESULT.RECORD_REPLACED || triggerChanged == ORecordHook.RESULT.RECORD_REPLACED) {
      ORecord replaced = OHookReplacedRecordThreadLocal.INSTANCE.get();
      if (replaced instanceof ODocument) {
        ((ODocument) replaced).validate();
      }
      return replaced;
    }

    if (changed) {
      return id;
    }
    return null;
  }

  /**
   * Deletes a document. Behavior depends by the current running transaction if any. If no transaction is running then the record is
   * deleted immediately. If an Optimistic transaction is running then the record will be deleted at commit time. The current
   * transaction will continue to see the record as deleted, while others not. If a Pessimistic transaction is running, then an
   * exclusive lock is acquired against the record. Current transaction will continue to see the record as deleted, while others
   * cannot access to it since it's locked.
   * <p>
   * If MVCC is enabled and the version of the document is different by the version stored in the database, then a {@link
   * OConcurrentModificationException} exception is thrown.
   *
   * @param record record to delete
   *
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   *
   * @see #setMVCC(boolean), {@link #isMVCC()}
   */
  public ODatabaseDocumentAbstract delete(final ORecord record) {
    checkOpenness();
    if (record == null)
      throw new ODatabaseException("Cannot delete null document");
    if (record instanceof OVertex) {
      OVertexDelegate.deleteLinks((OVertex) record);
    } else if (record instanceof OEdge) {
      OEdgeDelegate.deleteLinks((OEdge) record);
    }

    // CHECK ACCESS ON SCHEMA CLASS NAME (IF ANY)
    if (record instanceof ODocument && ((ODocument) record).getClassName() != null)
      checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_DELETE, ((ODocument) record).getClassName());

    try {
      currentTx.deleteRecord(record, OPERATION_MODE.SYNCHRONOUS);
    } catch (OException e) {
      throw e;
    } catch (Exception e) {
      if (record instanceof ODocument)
        throw OException.wrapException(new ODatabaseException(
            "Error on deleting record " + record.getIdentity() + " of class '" + ((ODocument) record).getClassName() + "'"), e);
      else
        throw OException.wrapException(new ODatabaseException("Error on deleting record " + record.getIdentity()), e);
    }
    return this;
  }

  @Override
  public void beforeDeleteOperations(OIdentifiable id, String iClusterName) {
    checkClusterSecurity(ORole.PERMISSION_DELETE, id, iClusterName);
    if (id instanceof ODocument) {
      ODocument doc = (ODocument) id;
      OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(this, doc);
      if (clazz != null) {
        if (clazz.isTriggered()) {
          OClassTrigger.onRecordBeforeDelete(doc, this);
        }
        if (clazz.isRestricted()) {
          if (!ORestrictedAccessHook.isAllowed(this, doc, ORestrictedOperation.ALLOW_DELETE, true))
            throw new OSecurityException("Cannot delete record " + doc.getIdentity() + ": the resource has restricted access");
        }
      }
    }
    callbackHooks(ORecordHook.TYPE.BEFORE_DELETE, id);
  }

  public void afterCreateOperations(final OIdentifiable id) {
    if (id instanceof ODocument) {
      ODocument doc = (ODocument) id;
      OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(this, doc);
      if (clazz != null) {
        OClassIndexManager.checkIndexesAfterCreate(doc, this);
        if (clazz.isFunction()) {
          this.getSharedContext().getFunctionLibrary().createdFunction(doc);
          Orient.instance().getScriptManager().close(this.getName());
        }
        if (clazz.isOuser() || clazz.isOrole()) {
          sharedContext.getSecurity().incrementVersion(this);
        }
        if (clazz.isSequence()) {
          ((OSequenceLibraryProxy) getMetadata().getSequenceLibrary()).getDelegate().onSequenceCreated(this, doc);
        }
        if (clazz.isScheduler()) {
          getMetadata().getScheduler().scheduleEvent(new OScheduledEvent(doc));
        }
        if (clazz.isTriggered()) {
          OClassTrigger.onRecordAfterCreate(doc, this);
        }

        getSharedContext().getViewManager().recordAdded(clazz, doc, this);
      }

      OLiveQueryHook.addOp(doc, ORecordOperation.CREATED, this);
      OLiveQueryHookV2.addOp(doc, ORecordOperation.CREATED, this);
    }
    callbackHooks(ORecordHook.TYPE.AFTER_CREATE, id);
  }

  public void afterUpdateOperations(final OIdentifiable id) {
    if (id instanceof ODocument) {
      ODocument doc = (ODocument) id;
      OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(this, doc);
      if (clazz != null) {
        OClassIndexManager.checkIndexesAfterUpdate((ODocument) id, this);
        if (clazz.isFunction()) {
          this.getSharedContext().getFunctionLibrary().updatedFunction(doc);
          Orient.instance().getScriptManager().close(this.getName());
        }
        if (clazz.isOuser() || clazz.isOrole()) {
          sharedContext.getSecurity().incrementVersion(this);
        }
        if (clazz.isSequence()) {
          ((OSequenceLibraryProxy) getMetadata().getSequenceLibrary()).getDelegate().onSequenceUpdated(this, doc);
        }
        if (clazz.isTriggered()) {
          OClassTrigger.onRecordAfterUpdate(doc, this);
        }

        getSharedContext().getViewManager().recordUpdated(clazz, doc, this);
      }
      OLiveQueryHook.addOp(doc, ORecordOperation.UPDATED, this);
      OLiveQueryHookV2.addOp(doc, ORecordOperation.UPDATED, this);

    }
    callbackHooks(ORecordHook.TYPE.AFTER_UPDATE, id);
  }

  public void afterDeleteOperations(final OIdentifiable id) {
    if (id instanceof ODocument) {
      ODocument doc = (ODocument) id;
      OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(this, doc);
      if (clazz != null) {
        OClassIndexManager.checkIndexesAfterDelete(doc, this);
        if (clazz.isFunction()) {
          this.getSharedContext().getFunctionLibrary().droppedFunction(doc);
          Orient.instance().getScriptManager().close(this.getName());
        }
        if (clazz.isOuser() || clazz.isOrole()) {
          sharedContext.getSecurity().incrementVersion(this);
        }
        if (clazz.isSequence()) {
          ((OSequenceLibraryProxy) getMetadata().getSequenceLibrary()).getDelegate().onSequenceDropped(this, doc);
        }
        if (clazz.isScheduler()) {
          final String eventName = doc.field(OScheduledEvent.PROP_NAME);
          getSharedContext().getScheduler().removeEventInternal(eventName);
        }
        if (clazz.isTriggered()) {
          OClassTrigger.onRecordAfterDelete(doc, this);
        }
        getSharedContext().getViewManager().recordDeleted(clazz, doc, this);
      }
      OLiveQueryHook.addOp(doc, ORecordOperation.DELETED, this);
      OLiveQueryHookV2.addOp(doc, ORecordOperation.DELETED, this);
    }
    callbackHooks(ORecordHook.TYPE.AFTER_DELETE, id);
  }

  @Override
  public void afterReadOperations(OIdentifiable identifiable) {
    if (identifiable instanceof ODocument) {
      ODocument doc = (ODocument) identifiable;
      OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(this, doc);
      if (clazz != null) {
        if (clazz.isTriggered()) {
          OClassTrigger.onRecordAfterRead(doc, this);
        }
      }
    }
    callbackHooks(ORecordHook.TYPE.AFTER_READ, identifiable);
  }

  @Override
  public boolean beforeReadOperations(OIdentifiable identifiable) {
    if (identifiable instanceof ODocument) {
      ODocument doc = (ODocument) identifiable;
      OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(this, doc);
      if (clazz != null) {
        if (clazz.isTriggered()) {
          ORecordHook.RESULT val = OClassTrigger.onRecordBeforeRead(doc, this);
          if (val == ORecordHook.RESULT.SKIP) {
            return true;
          }
        }
        if (clazz.isRestricted()) {
          if (!ORestrictedAccessHook.isAllowed(this, doc, ORestrictedOperation.ALLOW_READ, false)) {
            return true;
          }
        }
        ODocumentInternal.setPropertyAccess(doc, new OPropertyAccess(doc, getSharedContext().getSecurity()));
        ODocumentInternal.setPropertyEncryption(doc, new OPropertyEncryption());
      }
    }
    return callbackHooks(ORecordHook.TYPE.BEFORE_READ, identifiable) == ORecordHook.RESULT.SKIP;
  }

  @Override
  protected void afterCommitOperations() {
    super.afterCommitOperations();
    OLiveQueryHook.notifyForTxChanges(this);
    OLiveQueryHookV2.notifyForTxChanges(this);
  }

  @Override
  protected void afterRollbackOperations() {
    super.afterRollbackOperations();
    OLiveQueryHook.removePendingDatabaseOps(this);
    OLiveQueryHookV2.removePendingDatabaseOps(this);
  }

  @Override
  public ORecord saveAll(ORecord iRecord, String iClusterName, OPERATION_MODE iMode, boolean iForceCreate,
      ORecordCallback<? extends Number> iRecordCreatedCallback, ORecordCallback<Integer> iRecordUpdatedCallback) {

    ORecord toRet = null;
    ODirtyManager dirtyManager = ORecordInternal.getDirtyManager(iRecord);
    Set<ORecord> newRecord = dirtyManager.getNewRecords();
    Set<ORecord> updatedRecord = dirtyManager.getUpdateRecords();
    dirtyManager.clearForSave();
    if (iRecord.getIdentity().isNew()) {
      if (newRecord == null)
        newRecord = Collections.newSetFromMap(new IdentityHashMap<>());
      newRecord.add(iRecord);
    } else {
      if (updatedRecord == null)
        updatedRecord = Collections.newSetFromMap(new IdentityHashMap<>());
      updatedRecord.add(iRecord);
    }

    final OMicroTransaction microTx = beginMicroTransaction();
    try {
      if (newRecord != null) {
        for (ORecord rn : newRecord) {
          String cluster;
          if (iRecord == rn)
            cluster = iClusterName;
          else
            cluster = getClusterName(rn);
          microTx.saveRecord(rn, cluster, iMode, iForceCreate, iRecordCreatedCallback, iRecordUpdatedCallback);
        }
      }
      if (updatedRecord != null) {
        for (ORecord rn : updatedRecord) {
          microTx.saveRecord(rn, iClusterName, iMode, iForceCreate, iRecordCreatedCallback, iRecordUpdatedCallback);
        }
      }
    } catch (Exception e) {
      endMicroTransaction(false);
      throw e;
    }
    endMicroTransaction(true);

    return iRecord;
  }

  public String getClusterName(final ORecord record) {
    int clusterId = record.getIdentity().getClusterId();
    if (clusterId == ORID.CLUSTER_ID_INVALID) {
      // COMPUTE THE CLUSTER ID
      OClass schemaClass = null;
      if (record instanceof ODocument)
        schemaClass = ODocumentInternal.getImmutableSchemaClass(this, (ODocument) record);
      if (schemaClass != null) {
        // FIND THE RIGHT CLUSTER AS CONFIGURED IN CLASS
        if (schemaClass.isAbstract())
          throw new OSchemaException("Document belongs to abstract class '" + schemaClass.getName() + "' and cannot be saved");
        clusterId = schemaClass.getClusterForNewInstance((ODocument) record);
        return getClusterNameById(clusterId);
      } else {
        return getClusterNameById(getStorage().getDefaultClusterId());
      }

    } else {
      return getClusterNameById(clusterId);
    }
  }

  @Override
  public OView getViewFromCluster(int cluster) {
    OImmutableSchema schema = getMetadata().getImmutableSchemaSnapshot();
    OView view = schema.getViewByClusterId(cluster);
    if (view == null) {
      String viewName = getSharedContext().getViewManager().getViewFromOldCluster(cluster);
      if (viewName != null) {
        view = schema.getView(viewName);
      }
    }
    return view;
  }

  /**
   * This method is internal, it can be subject to signature change or be removed, do not use.
   *
   * @Internal
   */
  public <RET extends ORecord> RET executeReadRecord(final ORecordId rid, ORecord iRecord, final int recordVersion,
      final String fetchPlan, final boolean ignoreCache, final boolean iUpdateCache, final boolean loadTombstones,
      final OStorage.LOCKING_STRATEGY lockingStrategy, RecordReader recordReader) {
    checkOpenness();
    checkIfActive();

    getMetadata().makeThreadLocalSchemaSnapshot();
    ORecordSerializationContext.pushContext();
    try {
      checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, getClusterNameById(rid.getClusterId()));

      // either regular or micro tx must be active or both inactive
      assert !(getTransaction().isActive() && (microTransaction != null && microTransaction.isActive()));

      // SEARCH IN LOCAL TX
      ORecord record = getTransaction().getRecord(rid);
      if (record == OBasicTransaction.DELETED_RECORD)
        // DELETED IN TX
        return null;

      if (record == null) {
        if (microTransaction != null && microTransaction.isActive()) {
          record = microTransaction.getRecord(rid);
          if (record == OBasicTransaction.DELETED_RECORD)
            return null;
        }
      }

      if (record == null && !ignoreCache)
        // SEARCH INTO THE CACHE
        record = getLocalCache().findRecord(rid);

      if (record != null) {
        if (iRecord != null) {
          iRecord.fromStream(record.toStream());
          ORecordInternal.setVersion(iRecord, record.getVersion());
          record = iRecord;
        }

        OFetchHelper.checkFetchPlanValid(fetchPlan);
        if (beforeReadOperations(record))
          return null;

        if (record.getInternalStatus() == ORecordElement.STATUS.NOT_LOADED)
          record.reload();

        if (lockingStrategy == OStorage.LOCKING_STRATEGY.KEEP_SHARED_LOCK) {
          OLogManager.instance()
              .warn(this, "You use deprecated record locking strategy: %s it may lead to deadlocks " + lockingStrategy);
          record.lock(false);

        } else if (lockingStrategy == OStorage.LOCKING_STRATEGY.KEEP_EXCLUSIVE_LOCK) {
          OLogManager.instance()
              .warn(this, "You use deprecated record locking strategy: %s it may lead to deadlocks " + lockingStrategy);
          record.lock(true);
        }

        afterReadOperations(record);
        if (record instanceof ODocument)
          ODocumentInternal.checkClass((ODocument) record, this);
        return (RET) record;
      }

      final ORawBuffer recordBuffer;
      if (!rid.isValid())
        recordBuffer = null;
      else {
        OFetchHelper.checkFetchPlanValid(fetchPlan);

        int version;
        if (iRecord != null)
          version = iRecord.getVersion();
        else
          version = recordVersion;

        recordBuffer = recordReader.readRecord(getStorage(), rid, fetchPlan, ignoreCache, version);
      }

      if (recordBuffer == null)
        return null;

      if (iRecord == null || ORecordInternal.getRecordType(iRecord) != recordBuffer.recordType)
        // NO SAME RECORD TYPE: CAN'T REUSE OLD ONE BUT CREATE A NEW ONE FOR IT
        iRecord = Orient.instance().getRecordFactoryManager().newInstance(recordBuffer.recordType, rid.getClusterId(), this);

      ORecordInternal.setRecordSerializer(iRecord, getSerializer());
      ORecordInternal.fill(iRecord, rid, recordBuffer.version, recordBuffer.buffer, false, this);

      if (iRecord instanceof ODocument)
        ODocumentInternal.checkClass((ODocument) iRecord, this);

      if (ORecordVersionHelper.isTombstone(iRecord.getVersion()))
        return (RET) iRecord;

      if (beforeReadOperations(iRecord))
        return null;

      iRecord.fromStream(recordBuffer.buffer);

      afterReadOperations(iRecord);
      if (iUpdateCache)
        getLocalCache().updateRecord(iRecord);

      return (RET) iRecord;
    } catch (OOfflineClusterException t) {
      throw t;
    } catch (ORecordNotFoundException t) {
      throw t;
    } catch (Exception t) {
      if (rid.isTemporary())
        throw OException.wrapException(new ODatabaseException("Error on retrieving record using temporary RID: " + rid), t);
      else
        throw OException.wrapException(new ODatabaseException(
            "Error on retrieving record " + rid + " (cluster: " + getStorage().getPhysicalClusterNameById(rid.getClusterId())
                + ")"), t);
    } finally {
      ORecordSerializationContext.pullContext();
      getMetadata().clearThreadLocalSchemaSnapshot();
    }
  }

  @Override
  public void internalLockRecord(OIdentifiable iRecord, OStorage.LOCKING_STRATEGY lockingStrategy) {
    internalLockRecord(iRecord, lockingStrategy, 0);
  }

  public void internalLockRecord(OIdentifiable iRecord, OStorage.LOCKING_STRATEGY lockingStrategy, long timeout) {
    final ORID rid = new ORecordId(iRecord.getIdentity());
    OTransactionAbstract transaction = (OTransactionAbstract) getTransaction();
    if (!transaction.isLockedRecord(iRecord)) {
      if (lockingStrategy == OStorage.LOCKING_STRATEGY.EXCLUSIVE_LOCK)
        ((OAbstractPaginatedStorage) getStorage().getUnderlying()).acquireWriteLock(rid, timeout);
      else if (lockingStrategy == OStorage.LOCKING_STRATEGY.SHARED_LOCK)
        ((OAbstractPaginatedStorage) getStorage().getUnderlying()).acquireReadLock(rid, timeout);
      else
        throw new IllegalStateException("Unsupported locking strategy " + lockingStrategy);
    }
    transaction.trackLockedRecord(iRecord.getIdentity(), lockingStrategy);

  }

  @Override
  public void internalUnlockRecord(OIdentifiable iRecord) {
    final ORID rid = iRecord.getIdentity();
    OTransactionAbstract transaction = (OTransactionAbstract) getTransaction();
    OStorage.LOCKING_STRATEGY strategy = transaction.trackUnlockRecord(rid);

    if (strategy == OStorage.LOCKING_STRATEGY.EXCLUSIVE_LOCK)
      ((OAbstractPaginatedStorage) getStorage().getUnderlying()).releaseWriteLock(rid);
    else if (strategy == OStorage.LOCKING_STRATEGY.SHARED_LOCK)
      ((OAbstractPaginatedStorage) getStorage().getUnderlying()).releaseReadLock(rid);

  }

  @Override
  public <RET extends ORecord> RET lock(ORID recordId) throws OLockException {
    checkOpenness();
    checkIfActive();
    pessimisticLockChecks(recordId);
    internalLockRecord(recordId, OStorage.LOCKING_STRATEGY.EXCLUSIVE_LOCK);
    return load(recordId, null, true);
  }

  @Override
  public <RET extends ORecord> RET lock(ORID recordId, long timeout, TimeUnit timeoutUnit) throws OLockException {
    checkOpenness();
    checkIfActive();
    pessimisticLockChecks(recordId);
    internalLockRecord(recordId, OStorage.LOCKING_STRATEGY.EXCLUSIVE_LOCK, timeoutUnit.toMillis(timeout));
    return load(recordId, null, true);
  }

  @Override
  public void unlock(ORID recordId) throws OLockException {
    checkOpenness();
    checkIfActive();
    internalUnlockRecord(recordId);
  }

  @Override
  public <T> T sendSequenceAction(OSequenceAction action) throws ExecutionException, InterruptedException {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

}
