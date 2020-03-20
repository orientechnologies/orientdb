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
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.cache.OCommandCacheHook;
import com.orientechnologies.orient.core.cache.OLocalRecordCache;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.OScriptExecutor;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.record.OClassTrigger;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.*;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OClassIndexManager;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.function.OFunctionLibraryImpl;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.security.OImmutableUser;
import com.orientechnologies.orient.core.metadata.security.ORestrictedAccessHook;
import com.orientechnologies.orient.core.metadata.security.ORestrictedOperation;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibraryProxy;
import com.orientechnologies.orient.core.query.live.OLiveQueryHook;
import com.orientechnologies.orient.core.query.live.OLiveQueryHookV2;
import com.orientechnologies.orient.core.query.live.OLiveQueryListenerV2;
import com.orientechnologies.orient.core.query.live.OLiveQueryMonitorEmbedded;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.record.impl.OEdgeDelegate;
import com.orientechnologies.orient.core.record.impl.OVertexDelegate;
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
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OMicroTransaction;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordSerializationContext;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.core.tx.OTransactionData;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.concurrent.Callable;

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
  public void internalCreate(OrientDBConfig config) {
    this.status = STATUS.OPEN;
    // THIS IF SHOULDN'T BE NEEDED, CREATE HAPPEN ONLY IN EMBEDDED
    applyAttributes(config);
    applyListeners(config);
    metadata = new OMetadataDefault(this);
    installHooksEmbedded();
    createMetadata();

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
    ODatabaseDocumentEmbedded database = new ODatabaseDocumentEmbedded(storage);
    database.init(config);
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
    return status == STATUS.CLOSED || storage.isClosed();
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
    return microTransaction;
  }

  public static void deInit(OAbstractPaginatedStorage storage) {
    OSharedContext sharedContext = storage.removeResource(OSharedContext.class.getName());
    //This storage may not have been completely opened yet
    if (sharedContext != null)
      sharedContext.close();
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

  /**
   * This method is internal, it can be subject to signature change or be removed, do not use.
   *
   * @Internal
   */
  public <RET extends ORecord> RET executeSaveRecord(final ORecord record, String clusterName, final int ver,
      final OPERATION_MODE mode, boolean forceCreate, final ORecordCallback<? extends Number> recordCreatedCallback,
      ORecordCallback<Integer> recordUpdatedCallback) {

    checkOpenness();
    checkIfActive();
    if (!record.isDirty())
      return (RET) record;

    final ORecordId rid = (ORecordId) record.getIdentity();

    if (rid == null)
      throw new ODatabaseException(
          "Cannot create record because it has no identity. Probably is not a regular record or contains projections of fields rather than a full record");

    final OMicroTransaction microTx = beginMicroTransaction();
    try {
      microTx.saveRecord(record, clusterName, mode, forceCreate, recordCreatedCallback, recordUpdatedCallback);
    } catch (Exception e) {
      endMicroTransaction(false);
      throw e;
    }
    endMicroTransaction(true);
    return (RET) record;
  }

  private void endMicroTransaction(boolean success) {
    assert microTransaction != null;

    try {
      if (success)
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
      else {
        microTransaction.rollback();
        OLiveQueryHook.removePendingDatabaseOps(this);
        OLiveQueryHookV2.removePendingDatabaseOps(this);
      }
    } finally {
      if (!microTransaction.isActive())
        microTransaction = null;
    }
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
    } else if (record instanceof ODocument) {
      OElement elem = (OElement) record;
      if (elem.isVertex()) {
        OVertexDelegate.deleteLinks(elem.asVertex().get());
      } else if (elem.isEdge()) {
        OEdgeDelegate.deleteLinks(elem.asEdge().get());
      }
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
        if (clazz.isFunction()) {
          OFunctionLibraryImpl.validateFunctionRecord(doc);
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
        if (clazz.isFunction()) {
          OFunctionLibraryImpl.validateFunctionRecord(doc);
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
          getMetadata().getSecurity().incrementVersion();
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
          getMetadata().getSecurity().incrementVersion();
        }
        if (clazz.isSequence()) {
          ((OSequenceLibraryProxy) getMetadata().getSequenceLibrary()).getDelegate().onSequenceUpdated(this, doc);
        }
        if (clazz.isTriggered()) {
          OClassTrigger.onRecordAfterUpdate(doc, this);
        }
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
          getMetadata().getSecurity().incrementVersion();
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
  public void syncCommit(OTransactionData data) {
    OScenarioThreadLocal.executeAsDistributed(() -> {
      assert !this.getTransaction().isActive();
      OTransactionOptimistic tx = new OTransactionOptimistic(this);
      data.fill(tx, this);
      this.rawBegin(tx);
      this.commit();
      return null;
    });
  }
}
