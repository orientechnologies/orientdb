/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.listener.OListenerManger;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.cache.OCommandCacheHook;
import com.orientechnologies.orient.core.cache.OLocalRecordCache;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestInternal;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.record.*;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.ORidBagDeleter;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.exception.*;
import com.orientechnologies.orient.core.fetch.OFetchHelper;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.ClassIndexManagerRemote;
import com.orientechnologies.orient.core.index.OClassIndexManager;
import com.orientechnologies.orient.core.intent.OIntent;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.function.OFunctionTrigger;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;
import com.orientechnologies.orient.core.metadata.security.*;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceTrigger;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.query.live.OLiveQueryHook;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.record.impl.ODirtyManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.schedule.OSchedulerTrigger;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSaveThreadLocal;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import com.orientechnologies.orient.core.storage.*;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OFreezableStorageComponent;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OOfflineClusterException;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordSerializationContext;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionNoTx;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.core.tx.OTransactionRealAbstract;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Document API entrypoint.
 *
 * @author Luca Garulli
 */
@SuppressWarnings("unchecked")
public class ODatabaseDocumentTx extends OListenerManger<ODatabaseListener> implements ODatabaseDocumentInternal {

  @Deprecated
  private static final String DEF_RECORD_FORMAT = "csv";
  protected static ORecordSerializer defaultSerializer;

  static {
    defaultSerializer = ORecordSerializerFactory.instance()
        .getFormat(OGlobalConfiguration.DB_DOCUMENT_SERIALIZER.getValueAsString());
    if (defaultSerializer == null)
      throw new ODatabaseException(
          "Impossible to find serializer with name " + OGlobalConfiguration.DB_DOCUMENT_SERIALIZER.getValueAsString());
  }

  private final Map<String, Object> properties = new HashMap<String, Object>();
  private final Map<ORecordHook, ORecordHook.HOOK_POSITION> unmodifiableHooks;
  private final Set<OIdentifiable> inHook = new HashSet<OIdentifiable>();
  protected ORecordSerializer    serializer;
  private   String               url;
  private   OStorage             storage;
  private   STATUS               status;
  private   OIntent              currentIntent;
  private   ODatabaseInternal<?> databaseOwner;
  private   OMetadataDefault     metadata;
  private   OImmutableUser       user;
  private   byte                 recordType;
  @Deprecated
  private   String               recordFormat;
  private final Map<ORecordHook, ORecordHook.HOOK_POSITION> hooks         = new LinkedHashMap<ORecordHook, ORecordHook.HOOK_POSITION>();
  private       boolean                                     retainRecords = true;
  private OLocalRecordCache                localCache;
  private boolean                          mvcc;
  private OCurrentStorageComponentsFactory componentsFactory;
  private boolean initialized = false;
  private OTransaction currentTx;
  private       boolean                 keepStorageOpen = false;
  private final AtomicReference<Thread> owner           = new AtomicReference<Thread>();

  private boolean prefetchRecords = false;

  protected ODatabaseSessionMetadata sessionMetadata;

  private final ORecordHook[][] hooksByScope = new ORecordHook[ORecordHook.SCOPE.values().length][];

  /**
   * Creates a new connection to the database.
   *
   * @param iURL of the database
   */
  public ODatabaseDocumentTx(final String iURL) {
    this(iURL, false);
  }

  public ODatabaseDocumentTx(final String iURL, boolean keepStorageOpen) {
    super(false);

    if (iURL == null)
      throw new IllegalArgumentException("URL parameter is null");

    activateOnCurrentThread();

    try {
      this.keepStorageOpen = keepStorageOpen;
      url = iURL.replace('\\', '/');
      status = STATUS.CLOSED;

      // SET DEFAULT PROPERTIES
      setProperty("fetch-max", 50);

      storage = Orient.instance().loadStorage(url);

      // OVERWRITE THE URL
      url = storage.getURL();

      unmodifiableHooks = Collections.unmodifiableMap(hooks);

      recordType = ODocument.RECORD_TYPE;
      localCache = new OLocalRecordCache();

      mvcc = OGlobalConfiguration.DB_MVCC.getValueAsBoolean();

      init();

      databaseOwner = this;
    } catch (Exception t) {
      if (storage != null)
        Orient.instance().unregisterStorage(storage);
      ODatabaseRecordThreadLocal.INSTANCE.remove();

      throw OException.wrapException(new ODatabaseException("Error on opening database '" + iURL + "'"), t);
    }

    setSerializer(defaultSerializer);
  }

  /**
   * @return default serializer which is used to serialize documents. Default serializer is common for all database instances.
   */
  public static ORecordSerializer getDefaultSerializer() {
    return defaultSerializer;
  }

  /**
   * Sets default serializer. The default serializer is common for all database instances.
   *
   * @param iDefaultSerializer new default serializer value
   */
  public static void setDefaultSerializer(ORecordSerializer iDefaultSerializer) {
    defaultSerializer = iDefaultSerializer;
  }

  /**
   * Opens connection to the storage with given user and password.
   * <p/>
   * But we do suggest {@link com.orientechnologies.orient.core.db.OPartitionedDatabasePool#acquire()} instead. It will make work
   * faster even with embedded database.
   *
   * @param iUserName     Username to login
   * @param iUserPassword Password associated to the user
   *
   * @return Current database instance.
   */
  @Override
  public <DB extends ODatabase> DB open(final String iUserName, final String iUserPassword) {
    boolean failure = true;
    setupThreadOwner();
    activateOnCurrentThread();

    try {
      if (status == STATUS.OPEN)
        throw new IllegalStateException("Database " + getName() + " is already open");

      if (user != null && !user.getName().equals(iUserName))
        initialized = false;

      final String encKey = (String) getProperty(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey());
      String currKey = null;

      if (storage.getConfiguration() != null && storage.getConfiguration().getContextConfiguration() != null) {
        currKey = (String) storage.getConfiguration().getContextConfiguration()
            .getValue(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY);

        // If an encryption key is set as a database property, and
        // the storage engine is open and has an encryption key value, and
        // the two encryption keys differ, force the storage closed so that the
        // new encryption key in properties will be used.
        if (encKey != null && currKey != null && !encKey.equals(currKey)) {
          // If the storage is open...
          if (!storage.isClosed()) {
            storage.close(true, false); // force it closed
          }
        }
      }

      if (storage.isClosed()) {
        storage.open(iUserName, iUserPassword, properties);
        initialized = false;
      } else if (storage instanceof OStorageProxy) {
        final String name = ((OStorageProxy) storage).getUserName();
        if (name == null || !name.equals(iUserName)) {
          storage.close();
          storage.open(iUserName, iUserPassword, properties);
        }
      }

      status = STATUS.OPEN;

      initAtFirstOpen(iUserName, iUserPassword);

      if (!(getStorage() instanceof OStorageProxy)) {
        final OSecurity security = metadata.getSecurity();
        if (user == null || user.getVersion() != security.getVersion() || !user.getName().equalsIgnoreCase(iUserName)) {
          final OUser usr = metadata.getSecurity().authenticate(iUserName, iUserPassword);
          if (usr != null)
            user = new OImmutableUser(security.getVersion(), usr);
          else
            user = null;

          checkSecurity(ORule.ResourceGeneric.DATABASE, ORole.PERMISSION_READ);
        }
      }

      // WAKE UP LISTENERS
      callOnOpenListeners();

      failure = false;
    } catch (OException e) {
      close();
      throw e;
    } catch (Exception e) {
      close();
      throw OException.wrapException(new ODatabaseException("Cannot open database url=" + getURL()), e);
    } finally {
      if (failure)
        owner.set(null);
    }
    return (DB) this;
  }

  public boolean isPrefetchRecords() {
    return prefetchRecords;
  }

  public void setPrefetchRecords(boolean prefetchRecords) {
    this.prefetchRecords = prefetchRecords;
  }

  /**
   * Opens a database using an authentication token received as an argument.
   *
   * @param iToken Authentication token
   *
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  public <DB extends ODatabase> DB open(final OToken iToken) {
    boolean failure = true;

    setupThreadOwner();
    activateOnCurrentThread();

    try {
      if (status == STATUS.OPEN)
        throw new IllegalStateException("Database " + getName() + " is already open");

      if (user != null && !user.getIdentity().equals(iToken.getUserId()))
        initialized = false;

      if (storage instanceof OStorageProxy) {
        throw new ODatabaseException("Cannot use a token open on remote database");
      }
      if (storage.isClosed()) {
        // i don't have username and password at this level, anyway the storage embedded don't really need it
        storage.open("", "", properties);
      }

      status = STATUS.OPEN;

      initAtFirstOpen(null, null);

      final OSecurity security = metadata.getSecurity();
      if (user == null || user.getVersion() != security.getVersion()) {
        final OUser usr = metadata.getSecurity().authenticate(iToken);
        if (usr != null)
          user = new OImmutableUser(security.getVersion(), usr);
        else
          user = null;

        checkSecurity(ORule.ResourceGeneric.DATABASE, ORole.PERMISSION_READ);
      }

      // WAKE UP LISTENERS
      callOnOpenListeners();

      failure = false;
    } catch (OException e) {
      close();
      throw e;
    } catch (Exception e) {
      close();
      throw OException.wrapException(new ODatabaseException("Cannot open database"), e);
    } finally {
      if (failure)
        owner.set(null);
    }
    return (DB) this;
  }

  private void setupThreadOwner() {
    final Thread current = Thread.currentThread();
    final Thread o = owner.get();

    if (o != null || !owner.compareAndSet(null, current)) {
      throw new IllegalStateException("Current instance is owned by other thread" + (o != null ? " : '" + o.getName() + "'" : ""));
    }
  }

  public void callOnOpenListeners() {
    // WAKE UP DB LIFECYCLE LISTENER
    for (Iterator<ODatabaseLifecycleListener> it = Orient.instance().getDbLifecycleListeners(); it.hasNext(); )
      it.next().onOpen(getDatabaseOwner());

    // WAKE UP LISTENERS
    for (ODatabaseListener listener : getListenersCopy())
      try {
        listener.onOpen(getDatabaseOwner());
      } catch (Throwable t) {
        t.printStackTrace();
      }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <DB extends ODatabase> DB create() {
    return create((Map<OGlobalConfiguration, Object>) null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <DB extends ODatabase> DB create(String incrementalBackupPath) {
    create();

    final OStorage storage = getStorage();
    storage.restoreFromIncrementalBackup(incrementalBackupPath);

    metadata = new OMetadataDefault(this);
    metadata.load();

    return (DB) this;
  }

  @Override
  public <DB extends ODatabase> DB create(final Map<OGlobalConfiguration, Object> iInitialSettings) {
    setupThreadOwner();
    activateOnCurrentThread();

    try {
      if (status == STATUS.OPEN)
        throw new IllegalStateException("Database " + getName() + " is already open");

      if (storage == null)
        storage = Orient.instance().loadStorage(url);

      final OContextConfiguration ctxCfg = storage.getConfiguration().getContextConfiguration();
      if (iInitialSettings != null && !iInitialSettings.isEmpty()) {
        // SETUP INITIAL SETTINGS
        for (Map.Entry<OGlobalConfiguration, Object> e : iInitialSettings.entrySet()) {
          ctxCfg.setValue(e.getKey(), e.getValue());
        }
        storage.getConfiguration().update();
      }

      storage.create(properties);

      status = STATUS.OPEN;

      componentsFactory = getStorage().getComponentsFactory();

      localCache.startup();

      getStorage().getConfiguration().setRecordSerializer(getSerializer().toString());
      getStorage().getConfiguration().setRecordSerializerVersion(getSerializer().getCurrentVersion());

      // since 2.1 newly created databases use strict SQL validation by default
      getStorage().getConfiguration().setProperty(OStatement.CUSTOM_STRICT_SQL, "true");

      getStorage().getConfiguration().update();

      // THIS IF SHOULDN'T BE NEEDED, CREATE HAPPEN ONLY IN EMBEDDED
      if (!(getStorage() instanceof OStorageProxy))
        installHooksEmbedded();

      // CREATE THE DEFAULT SCHEMA WITH DEFAULT USER
      metadata = new OMetadataDefault(this);
      metadata.create();

      if (!(getStorage() instanceof OStorageProxy))
        registerHook(new OCommandCacheHook(this), ORecordHook.HOOK_POSITION.REGULAR);

      registerHook(new OSecurityTrackerHook(metadata.getSecurity(), this), ORecordHook.HOOK_POSITION.LAST);

      final OUser usr = getMetadata().getSecurity().getUser(OUser.ADMIN);

      if (usr == null)
        user = null;
      else
        user = new OImmutableUser(getMetadata().getSecurity().getVersion(), usr);

      // WAKE UP DB LIFECYCLE LISTENER
      for (Iterator<ODatabaseLifecycleListener> it = Orient.instance().getDbLifecycleListeners(); it.hasNext(); )
        it.next().onCreate(getDatabaseOwner());

      // WAKE UP LISTENERS
      for (ODatabaseListener listener : browseListeners())
        try {
          listener.onCreate(this);
        } catch (Throwable ignore) {
        }
    } catch (Exception e) {
      // REMOVE THE (PARTIAL) DATABASE
      try {
        drop();
      } catch (Exception ex) {
        // IGNORE IT
      }

      // DELETE THE STORAGE TOO
      try {
        if (storage == null)
          storage = Orient.instance().loadStorage(url);
        storage.delete();
      } catch (Exception ex) {
        // IGNORE IT
      }

      status = STATUS.CLOSED;
      owner.set(null);

      throw OException.wrapException(new ODatabaseException("Cannot create database '" + getName() + "'"), e);
    }
    return (DB) this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void drop() {
    checkOpeness();
    checkIfActive();

    checkSecurity(ORule.ResourceGeneric.DATABASE, ORole.PERMISSION_DELETE);

    callOnDropListeners();

    if (metadata != null) {
      metadata.close();
      metadata = null;
    }

    closeOnDelete();

    try {
      if (storage == null)
        storage = Orient.instance().loadStorage(url);

      storage.delete();
      storage = null;

      status = STATUS.CLOSED;
      ODatabaseRecordThreadLocal.INSTANCE.remove();
      clearOwner();

    } catch (OException e) {
      // PASS THROUGH
      throw e;
    } catch (Exception e) {
      throw OException.wrapException(new ODatabaseException("Cannot delete database"), e);
    }
  }

  /**
   * Returns a copy of current database if it's open. The returned instance can be used by another thread without affecting current
   * instance. The database copy is not set in thread local.
   */
  public ODatabaseDocumentTx copy() {
    ODatabaseDocumentInternal dbInThreadLocal = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (this.isClosed())
      throw new ODatabaseException("Cannot copy a closed db");

    final ODatabaseDocumentTx db = new ODatabaseDocumentTx(this.url);
    db.setupThreadOwner();

    db.user = this.user;
    db.properties.putAll(this.properties);
    db.serializer = this.serializer;

    db.componentsFactory = this.componentsFactory;
    db.metadata = new OMetadataDefault(db);

    db.initialized = true;
    if (storage instanceof OStorageProxy) {
      db.storage = ((OStorageProxy) storage).copy(this, db);
      ((OStorageProxy) db.storage).addUser();
    } else {
      db.storage = storage;
    }

    db.setStatus(STATUS.OPEN);
    db.metadata.load();

    if (!(db.getStorage() instanceof OStorageProxy))
      db.installHooksEmbedded();
    else
      db.installHooksRemote();

    db.initialized = true;

    if (dbInThreadLocal != null) {
      dbInThreadLocal.activateOnCurrentThread();
    } else {
      if (ODatabaseRecordThreadLocal.INSTANCE.isDefined()) {
        ODatabaseRecordThreadLocal.INSTANCE.remove();
      }
    }

    return db;
  }

  public void callOnCloseListeners() {
    // WAKE UP DB LIFECYCLE LISTENER
    for (Iterator<ODatabaseLifecycleListener> it = Orient.instance().getDbLifecycleListeners(); it.hasNext(); )
      it.next().onClose(getDatabaseOwner());

    // WAKE UP LISTENERS
    for (ODatabaseListener listener : getListenersCopy())
      try {
        listener.onClose(getDatabaseOwner());
      } catch (Throwable t) {
        t.printStackTrace();
      }
  }

  public void callOnDropListeners() {
    // WAKE UP DB LIFECYCLE LISTENER
    for (Iterator<ODatabaseLifecycleListener> it = Orient.instance().getDbLifecycleListeners(); it.hasNext(); ) {
      activateOnCurrentThread();
      it.next().onDrop(getDatabaseOwner());
    }

    // WAKE UP LISTENERS
    for (ODatabaseListener listener : getListenersCopy())
      try {
        activateOnCurrentThread();
        listener.onDelete(getDatabaseOwner());
      } catch (Throwable t) {
        t.printStackTrace();
      }
  }

  /**
   * {@inheritDoc}
   */
  public <RET extends ORecord> RET getRecord(final OIdentifiable iIdentifiable) {
    if (iIdentifiable instanceof ORecord)
      return (RET) iIdentifiable;
    return (RET) load(iIdentifiable.getIdentity());
  }

  @Override
  public void reload() {
    checkIfActive();

    if (this.isClosed())
      throw new ODatabaseException("Cannot reload a closed db");

    storage.reload();
    metadata.reload();
  }

  /**
   * {@inheritDoc}
   */
  public <RET extends ORecord> RET load(final ORID iRecordId, final String iFetchPlan, final boolean iIgnoreCache) {
    return (RET) executeReadRecord((ORecordId) iRecordId, null, -1, iFetchPlan, iIgnoreCache, !iIgnoreCache, false,
        OStorage.LOCKING_STRATEGY.DEFAULT, new SimpleRecordReader(prefetchRecords));
  }

  /**
   * Deletes the record checking the version.
   */
  public ODatabase<ORecord> delete(final ORID iRecord, final int iVersion) {
    executeDeleteRecord(iRecord, iVersion, true, OPERATION_MODE.SYNCHRONOUS, false);
    return this;
  }

  public ODatabase<ORecord> cleanOutRecord(final ORID iRecord, final int iVersion) {
    executeDeleteRecord(iRecord, iVersion, true, OPERATION_MODE.SYNCHRONOUS, true);
    return this;
  }

  public String getType() {
    return TYPE;
  }

  /**
   * Deletes the record without checking the version.
   */
  public ODatabaseDocument delete(final ORID iRecord, final OPERATION_MODE iMode) {
    ORecord record = iRecord.getRecord();
    if (record == null)
      return this;

    delete(record, iMode);
    return this;
  }

  public ODatabaseDocument delete(final ORecord iRecord, final OPERATION_MODE iMode) {
    checkIfActive();
    currentTx.deleteRecord(iRecord, iMode);
    return this;
  }

  public <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(final String iClusterName, final Class<REC> iClass) {
    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, iClusterName);

    checkIfActive();

    final int clusterId = getClusterIdByName(iClusterName);

    return new ORecordIteratorCluster<REC>(this, this, clusterId);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Deprecated
  public <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(final String iClusterName, final Class<REC> iRecordClass,
      final long startClusterPosition, final long endClusterPosition, final boolean loadTombstones) {
    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, iClusterName);
    checkIfActive();

    final int clusterId = getClusterIdByName(iClusterName);

    return new ORecordIteratorCluster<REC>(this, this, clusterId, startClusterPosition, endClusterPosition, loadTombstones,
        OStorage.LOCKING_STRATEGY.DEFAULT);
  }

  @Override
  public <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(String iClusterName, Class<REC> iRecordClass,
      long startClusterPosition, long endClusterPosition) {
    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, iClusterName);
    checkIfActive();

    final int clusterId = getClusterIdByName(iClusterName);

    return new ORecordIteratorCluster<REC>(this, this, clusterId, startClusterPosition, endClusterPosition);
  }

  /**
   * {@inheritDoc}
   */
  public OCommandRequest command(final OCommandRequest iCommand) {
    checkSecurity(ORule.ResourceGeneric.COMMAND, ORole.PERMISSION_READ);
    checkIfActive();

    final OCommandRequestInternal command = (OCommandRequestInternal) iCommand;

    try {
      command.reset();
      return command;

    } catch (Exception e) {
      throw OException.wrapException(new ODatabaseException("Error on command execution"), e);
    }
  }

  /**
   * {@inheritDoc}
   */
  public <RET extends List<?>> RET query(final OQuery<?> iCommand, final Object... iArgs) {
    checkIfActive();
    iCommand.reset();
    return (RET) iCommand.execute(iArgs);
  }

  /**
   * {@inheritDoc}
   */
  public byte getRecordType() {
    return recordType;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long countClusterElements(final int[] iClusterIds) {
    return countClusterElements(iClusterIds, false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long countClusterElements(final int iClusterId) {
    return countClusterElements(iClusterId, false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void truncateCluster(String clusterName) {
    command(new OCommandSQL("truncate cluster " + clusterName)).execute();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long countClusterElements(int iClusterId, boolean countTombstones) {
    final String name = getClusterNameById(iClusterId);
    if (name == null)
      return 0;

    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, name);
    checkIfActive();

    return storage.count(iClusterId, countTombstones);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long countClusterElements(int[] iClusterIds, boolean countTombstones) {
    checkIfActive();
    String name;
    for (int iClusterId : iClusterIds) {
      name = getClusterNameById(iClusterId);
      checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, name);
    }

    return storage.count(iClusterIds, countTombstones);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long countClusterElements(final String iClusterName) {
    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, iClusterName);
    checkIfActive();

    final int clusterId = getClusterIdByName(iClusterName);
    if (clusterId < 0)
      throw new IllegalArgumentException("Cluster '" + iClusterName + "' was not found");
    return storage.count(clusterId);
  }

  /**
   * {@inheritDoc}
   */
  public OMetadataDefault getMetadata() {
    checkOpeness();
    return metadata;
  }

  /**
   * {@inheritDoc}
   */
  public <DB extends ODatabaseDocument> DB checkSecurity(final ORule.ResourceGeneric resourceGeneric, final String resourceSpecific,
      final int iOperation) {
    if (user != null) {
      try {
        user.allow(resourceGeneric, resourceSpecific, iOperation);
      } catch (OSecurityAccessException e) {

        if (OLogManager.instance().isDebugEnabled())
          OLogManager.instance()
              .debug(this, "User '%s' tried to access the reserved resource '%s.%s', operation '%s'", getUser(), resourceGeneric,
                  resourceSpecific, iOperation);

        throw e;
      }
    }
    return (DB) this;
  }

  /**
   * {@inheritDoc}
   */
  public <DB extends ODatabaseDocument> DB checkSecurity(final ORule.ResourceGeneric iResourceGeneric, final int iOperation,
      final Object... iResourcesSpecific) {

    if (user != null) {
      try {
        if (iResourcesSpecific.length != 0) {
          for (Object target : iResourcesSpecific) {
            if (target != null) {
              user.allow(iResourceGeneric, target.toString(), iOperation);
            } else
              user.allow(iResourceGeneric, null, iOperation);
          }
        } else
          user.allow(iResourceGeneric, null, iOperation);
      } catch (OSecurityAccessException e) {
        if (OLogManager.instance().isDebugEnabled())
          OLogManager.instance()
              .debug(this, "[checkSecurity] User '%s' tried to access the reserved resource '%s', target(s) '%s', operation '%s'",
                  getUser(), iResourceGeneric, Arrays.toString(iResourcesSpecific), iOperation);

        throw e;
      }
    }
    return (DB) this;
  }

  /**
   * {@inheritDoc}
   */
  public <DB extends ODatabaseDocument> DB checkSecurity(final ORule.ResourceGeneric iResourceGeneric, final int iOperation,
      final Object iResourceSpecific) {
    checkOpeness();
    if (user != null) {
      try {
        if (iResourceSpecific != null)
          user.allow(iResourceGeneric, iResourceSpecific.toString(), iOperation);
        else
          user.allow(iResourceGeneric, null, iOperation);
      } catch (OSecurityAccessException e) {
        if (OLogManager.instance().isDebugEnabled())
          OLogManager.instance()
              .debug(this, "[checkSecurity] User '%s' tried to access the reserved resource '%s', target '%s', operation '%s'",
                  getUser(), iResourceGeneric, iResourceSpecific, iOperation);

        throw e;
      }
    }
    return (DB) this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ODatabaseInternal<?> getDatabaseOwner() {
    ODatabaseInternal<?> current = databaseOwner;

    while (current != null && current != this && current.getDatabaseOwner() != current)
      current = current.getDatabaseOwner();

    return current;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ODatabaseInternal<ORecord> setDatabaseOwner(ODatabaseInternal<?> iOwner) {
    databaseOwner = iOwner;
    return this;
  }

  /**
   * {@inheritDoc}
   */
  public boolean isRetainRecords() {
    return retainRecords;
  }

  /**
   * {@inheritDoc}
   */
  public ODatabaseDocument setRetainRecords(boolean retainRecords) {
    this.retainRecords = retainRecords;
    return this;
  }

  /**
   * {@inheritDoc}
   */
  public <DB extends ODatabase> DB setStatus(final STATUS status) {
    checkIfActive();
    setStatusInternal(status);
    return (DB) this;
  }

  public void setStatusInternal(final STATUS status) {
    this.status = status;
  }

  /**
   * Deprecated since v2.2
   */
  @Deprecated
  public void setDefaultClusterIdInternal(final int iDefClusterId) {
    checkIfActive();
    getStorage().setDefaultClusterId(iDefClusterId);
  }

  /**
   * {@inheritDoc}
   */
  public void setInternal(final ATTRIBUTES iAttribute, final Object iValue) {
    set(iAttribute, iValue);
  }

  /**
   * {@inheritDoc}
   */
  public OSecurityUser getUser() {
    return user;
  }

  /**
   * {@inheritDoc}
   */
  public void setUser(final OSecurityUser user) {
    checkIfActive();
    if (user instanceof OUser) {
      OMetadata metadata = getMetadata();
      if (metadata != null) {
        final OSecurity security = metadata.getSecurity();
        this.user = new OImmutableUser(security.getVersion(), (OUser) user);
      } else
        this.user = new OImmutableUser(-1, (OUser) user);
    } else
      this.user = (OImmutableUser) user;
  }

  public void reloadUser() {
    if (user != null) {
      activateOnCurrentThread();

      OMetadata metadata = getMetadata();

      if (metadata != null) {
        final OSecurity security = metadata.getSecurity();
        OUser secGetUser = security.getUser(user.getName());

        if (secGetUser != null)
          user = new OImmutableUser(security.getVersion(), secGetUser);
        else
          user = new OImmutableUser(-1, new OUser());
      } else
        user = new OImmutableUser(-1, new OUser());
    }
  }

  /**
   * {@inheritDoc}
   */
  public boolean isMVCC() {
    return mvcc;
  }

  /**
   * {@inheritDoc}
   */
  public <DB extends ODatabase<?>> DB setMVCC(boolean mvcc) {
    this.mvcc = mvcc;
    return (DB) this;
  }

  /**
   * {@inheritDoc}
   */
  public ODictionary<ORecord> getDictionary() {
    checkOpeness();
    return metadata.getIndexManager().getDictionary();
  }

  /**
   * {@inheritDoc}
   */
  public <DB extends ODatabase<?>> DB registerHook(final ORecordHook iHookImpl, final ORecordHook.HOOK_POSITION iPosition) {
    checkOpeness();
    checkIfActive();

    final Map<ORecordHook, ORecordHook.HOOK_POSITION> tmp = new LinkedHashMap<ORecordHook, ORecordHook.HOOK_POSITION>(hooks);
    tmp.put(iHookImpl, iPosition);
    hooks.clear();
    for (ORecordHook.HOOK_POSITION p : ORecordHook.HOOK_POSITION.values()) {
      for (Map.Entry<ORecordHook, ORecordHook.HOOK_POSITION> e : tmp.entrySet()) {
        if (e.getValue() == p)
          hooks.put(e.getKey(), e.getValue());
      }
    }

    compileHooks();

    return (DB) this;
  }

  /**
   * {@inheritDoc}
   */
  public <DB extends ODatabase<?>> DB registerHook(final ORecordHook iHookImpl) {
    return (DB) registerHook(iHookImpl, ORecordHook.HOOK_POSITION.REGULAR);
  }

  /**
   * {@inheritDoc}
   */
  public <DB extends ODatabase<?>> DB unregisterHook(final ORecordHook iHookImpl) {
    checkIfActive();
    if (iHookImpl != null) {
      iHookImpl.onUnregister();
      hooks.remove(iHookImpl);

      compileHooks();
    }
    return (DB) this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public OLocalRecordCache getLocalCache() {
    return localCache;
  }

  /**
   * {@inheritDoc}
   */
  public Map<ORecordHook, ORecordHook.HOOK_POSITION> getHooks() {
    return unmodifiableHooks;
  }

  /**
   * Callback the registered hooks if any.
   *
   * @param type Hook type. Define when hook is called.
   * @param id   Record received in the callback
   *
   * @return True if the input record is changed, otherwise false
   */
  public ORecordHook.RESULT callbackHooks(final ORecordHook.TYPE type, final OIdentifiable id) {
    if (id == null || hooks.isEmpty() || id.getIdentity().getClusterId() == 0)
      return ORecordHook.RESULT.RECORD_NOT_CHANGED;

    final ORecordHook.SCOPE scope = ORecordHook.SCOPE.typeToScope(type);
    final int scopeOrdinal = scope.ordinal();

    ORID identity = id.getIdentity().copy();
    if (!pushInHook(identity))
      return ORecordHook.RESULT.RECORD_NOT_CHANGED;

    try {
      final ORecord rec = id.getRecord();
      if (rec == null)
        return ORecordHook.RESULT.RECORD_NOT_CHANGED;

      final OScenarioThreadLocal.RUN_MODE runMode = OScenarioThreadLocal.INSTANCE.getRunMode();

      boolean recordChanged = false;
      for (ORecordHook hook : hooksByScope[scopeOrdinal]) {
        switch (runMode) {
        case DEFAULT: // NON_DISTRIBUTED OR PROXIED DB
          if (getStorage().isDistributed()
              && hook.getDistributedExecutionMode() == ORecordHook.DISTRIBUTED_EXECUTION_MODE.TARGET_NODE)
            // SKIP
            continue;
          break; // TARGET NODE
        case RUNNING_DISTRIBUTED:
          if (hook.getDistributedExecutionMode() == ORecordHook.DISTRIBUTED_EXECUTION_MODE.SOURCE_NODE)
            continue;
        }

        final ORecordHook.RESULT res = hook.onTrigger(type, rec);

        if (res == ORecordHook.RESULT.RECORD_CHANGED)
          recordChanged = true;
        else if (res == ORecordHook.RESULT.SKIP_IO)
          // SKIP IO OPERATION
          return res;
        else if (res == ORecordHook.RESULT.SKIP)
          // SKIP NEXT HOOKS AND RETURN IT
          return res;
        else if (res == ORecordHook.RESULT.RECORD_REPLACED)
          return res;
      }

      return recordChanged ? ORecordHook.RESULT.RECORD_CHANGED : ORecordHook.RESULT.RECORD_NOT_CHANGED;

    } finally {
      popInHook(identity);
    }
  }

  /**
   * {@inheritDoc}
   */
  public boolean isValidationEnabled() {
    return (Boolean) get(ATTRIBUTES.VALIDATION);
  }

  /**
   * {@inheritDoc}
   */
  public <DB extends ODatabaseDocument> DB setValidationEnabled(final boolean iEnabled) {
    set(ATTRIBUTES.VALIDATION, iEnabled);
    return (DB) this;
  }

  public ORecordConflictStrategy getConflictStrategy() {
    checkIfActive();
    return getStorage().getConflictStrategy();
  }

  public ODatabaseDocumentTx setConflictStrategy(final String iStrategyName) {
    checkIfActive();
    getStorage().setConflictStrategy(Orient.instance().getRecordConflictStrategy().getStrategy(iStrategyName));
    return this;
  }

  public ODatabaseDocumentTx setConflictStrategy(final ORecordConflictStrategy iResolver) {
    checkIfActive();
    getStorage().setConflictStrategy(iResolver);
    return this;
  }

  @Override
  public OContextConfiguration getConfiguration() {
    checkIfActive();
    if (storage != null)
      return storage.getConfiguration().getContextConfiguration();
    return null;
  }

  @Override
  public boolean declareIntent(final OIntent iIntent) {
    checkIfActive();

    if (currentIntent != null) {
      if (iIntent != null && iIntent.getClass().equals(currentIntent.getClass()))
        // SAME INTENT: JUMP IT
        return false;

      // END CURRENT INTENT
      currentIntent.end(this);
    }

    currentIntent = iIntent;

    if (iIntent != null)
      iIntent.begin(this);

    return true;
  }

  @Override
  public boolean exists() {
    if (status == STATUS.OPEN)
      return true;

    if (storage == null)
      storage = Orient.instance().loadStorage(url);

    return storage.exists();
  }

  @Override
  public void close() {
    checkIfActive();

    try {
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

      status = STATUS.CLOSED;

      localCache.clear();

      if (!keepStorageOpen && storage != null)
        storage.close();

    } finally {
      // ALWAYS RESET TL
      ODatabaseRecordThreadLocal.INSTANCE.remove();
      clearOwner();
    }
  }

  private void clearOwner() {
    owner.set(null);
  }

  @Override
  public STATUS getStatus() {
    return status;
  }

  @Override
  public long getSize() {
    checkIfActive();
    return storage.getSize();
  }

  @Override
  public String getName() {
    return storage != null ? storage.getName() : url;
  }

  @Override
  public String getURL() {
    return url != null ? url : storage.getURL();
  }

  @Override
  public int getDefaultClusterId() {
    checkIfActive();
    return storage.getDefaultClusterId();
  }

  @Override
  public int getClusters() {
    checkIfActive();
    return storage.getClusters();
  }

  @Override
  public boolean existsCluster(final String iClusterName) {
    checkIfActive();
    return storage.getClusterNames().contains(iClusterName.toLowerCase());
  }

  @Override
  public Collection<String> getClusterNames() {
    checkIfActive();
    return storage.getClusterNames();
  }

  @Override
  public int getClusterIdByName(final String iClusterName) {
    if (iClusterName == null)
      return -1;

    checkIfActive();
    return storage.getClusterIdByName(iClusterName.toLowerCase());
  }

  @Override
  public String getClusterNameById(final int iClusterId) {
    if (iClusterId < 0)
      return null;

    checkIfActive();
    return storage.getPhysicalClusterNameById(iClusterId);
  }

  @Override
  public long getClusterRecordSizeByName(final String clusterName) {
    checkIfActive();
    try {
      return storage.getClusterById(getClusterIdByName(clusterName)).getRecordsSize();
    } catch (Exception e) {
      throw OException.wrapException(new ODatabaseException("Error on reading records size for cluster '" + clusterName + "'"), e);
    }
  }

  @Override
  public long getClusterRecordSizeById(final int clusterId) {
    checkIfActive();
    try {
      return storage.getClusterById(clusterId).getRecordsSize();
    } catch (Exception e) {
      throw OException
          .wrapException(new ODatabaseException("Error on reading records size for cluster with id '" + clusterId + "'"), e);
    }
  }

  @Override
  public boolean isClosed() {
    return status == STATUS.CLOSED || storage.isClosed();
  }

  @Override
  public int addCluster(final String iClusterName, final Object... iParameters) {
    checkIfActive();

    checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_CREATE);

    return storage.addCluster(iClusterName, false, iParameters);
  }

  @Override
  public int addCluster(final String iClusterName, final int iRequestedId, final Object... iParameters) {
    checkIfActive();

    checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_CREATE);

    return storage.addCluster(iClusterName, iRequestedId, false, iParameters);
  }

  @Override
  public Object alterCluster(String iClusterName, OCluster.ATTRIBUTES attribute, Object value) {
    checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    OCluster cluster = storage.getClusterById(storage.getClusterIdByName(iClusterName));

    if (cluster == null)
      throw new ODatabaseException("Cannot alter cluster with name: " + iClusterName);

    Object result;

    try {
      if (attribute == OCluster.ATTRIBUTES.STATUS && OStorageClusterConfiguration.STATUS.OFFLINE.toString()
          .equalsIgnoreCase((String) value))
        // REMOVE CACHE OF COMMAND RESULTS IF ACTIVE
        getMetadata().getCommandCache().invalidateResultsOfCluster(iClusterName);

      if (attribute == OCluster.ATTRIBUTES.NAME)
        // REMOVE CACHE OF COMMAND RESULTS IF ACTIVE
        getMetadata().getCommandCache().invalidateResultsOfCluster(iClusterName);

      result = cluster.set(attribute, value);
    } catch (Exception ex) {
      throw OException.wrapException(new ODatabaseException("Error while altering cluster with name: " + iClusterName), ex);
    }

    return result;
  }

  @Override
  public Object alterCluster(int iClusterId, OCluster.ATTRIBUTES attribute, Object value) {
    checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    OCluster cluster = storage.getClusterById(iClusterId);

    if (cluster == null)
      throw new ODatabaseException("Cannot alter cluster with id: " + iClusterId);

    Object result;

    try {
      if (attribute == OCluster.ATTRIBUTES.STATUS && OStorageClusterConfiguration.STATUS.OFFLINE.toString()
          .equalsIgnoreCase((String) value))
        // REMOVE CACHE OF COMMAND RESULTS IF ACTIVE
        getMetadata().getCommandCache().invalidateResultsOfCluster(getClusterNameById(iClusterId));

      if (attribute == OCluster.ATTRIBUTES.NAME)
        // REMOVE CACHE OF COMMAND RESULTS IF ACTIVE
        getMetadata().getCommandCache().invalidateResultsOfCluster(getClusterNameById(iClusterId));

      result = cluster.set(attribute, value);
    } catch (Exception ex) {
      throw OException.wrapException(new ODatabaseException("Error while altering cluster with id: " + iClusterId), ex);
    }

    return result;
  }

  @Override
  public boolean dropCluster(final String iClusterName, final boolean iTruncate) {
    checkIfActive();

    checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_DELETE);

    final int clusterId = getClusterIdByName(iClusterName);
    OSchemaProxy schema = metadata.getSchema();
    OClass clazz = schema.getClassByClusterId(clusterId);
    if (clazz != null)
      clazz.removeClusterId(clusterId);
    if (schema.getBlobClusters().contains(clusterId))
      schema.removeBlobCluster(iClusterName);
    getLocalCache().freeCluster(clusterId);
    storage.checkForClusterPermissions(iClusterName);
    return storage.dropCluster(iClusterName, iTruncate);
  }

  @Override
  public boolean dropCluster(final int iClusterId, final boolean iTruncate) {
    checkIfActive();

    checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_DELETE);

    OSchemaProxy schema = metadata.getSchema();
    final OClass clazz = schema.getClassByClusterId(iClusterId);
    if (clazz != null)
      clazz.removeClusterId(iClusterId);
    getLocalCache().freeCluster(iClusterId);
    if (schema.getBlobClusters().contains(iClusterId))
      schema.removeBlobCluster(getClusterNameById(iClusterId));

    storage.checkForClusterPermissions(getClusterNameById(iClusterId));
    return storage.dropCluster(iClusterId, iTruncate);
  }

  @Override
  public Object setProperty(final String iName, final Object iValue) {
    if (iValue == null)
      return properties.remove(iName.toLowerCase());
    else
      return properties.put(iName.toLowerCase(), iValue);
  }

  @Override
  public Object getProperty(final String iName) {
    return properties.get(iName.toLowerCase());
  }

  @Override
  public Iterator<Map.Entry<String, Object>> getProperties() {
    return properties.entrySet().iterator();
  }

  @Override
  public Object get(final ATTRIBUTES iAttribute) {
    checkIfActive();

    if (iAttribute == null)
      throw new IllegalArgumentException("attribute is null");

    switch (iAttribute) {
    case STATUS:
      return getStatus();
    case DEFAULTCLUSTERID:
      return getDefaultClusterId();
    case TYPE:
      return getMetadata().getImmutableSchemaSnapshot().existsClass("V") ? "graph" : "document";
    case DATEFORMAT:
      return storage.getConfiguration().dateFormat;

    case DATETIMEFORMAT:
      return storage.getConfiguration().dateTimeFormat;

    case TIMEZONE:
      return storage.getConfiguration().getTimeZone().getID();

    case LOCALECOUNTRY:
      return storage.getConfiguration().getLocaleCountry();

    case LOCALELANGUAGE:
      return storage.getConfiguration().getLocaleLanguage();

    case CHARSET:
      return storage.getConfiguration().getCharset();

    case CUSTOM:
      return storage.getConfiguration().getProperties();

    case CLUSTERSELECTION:
      return storage.getConfiguration().getClusterSelection();

    case MINIMUMCLUSTERS:
      return storage.getConfiguration().getMinimumClusters();

    case CONFLICTSTRATEGY:
      return storage.getConfiguration().getConflictStrategy();

    case VALIDATION:
      return storage.getConfiguration().isValidationEnabled();
    }

    return null;
  }

  @Override
  public <DB extends ODatabase> DB set(final ATTRIBUTES iAttribute, final Object iValue) {
    checkIfActive();

    if (iAttribute == null)
      throw new IllegalArgumentException("attribute is null");

    final String stringValue = OIOUtils.getStringContent(iValue != null ? iValue.toString() : null);

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

      storage.getConfiguration().dateFormat = stringValue;
      storage.getConfiguration().update();
      break;

    case DATETIMEFORMAT:
      if (stringValue == null)
        throw new IllegalArgumentException("date format is null");

      // CHECK FORMAT
      new SimpleDateFormat(stringValue).format(new Date());

      storage.getConfiguration().dateTimeFormat = stringValue;
      storage.getConfiguration().update();
      break;

    case TIMEZONE:
      if (stringValue == null)
        throw new IllegalArgumentException("Timezone can't be null");

      // for backward compatibility, until 2.1.13 OrientDB accepted timezones in lowercase as well
      TimeZone timeZoneValue = TimeZone.getTimeZone(stringValue.toUpperCase());
      if (timeZoneValue.equals(TimeZone.getTimeZone("GMT"))) {
        timeZoneValue = TimeZone.getTimeZone(stringValue);
      }

      storage.getConfiguration().setTimeZone(timeZoneValue);
      storage.getConfiguration().update();
      break;

    case LOCALECOUNTRY:
      storage.getConfiguration().setLocaleCountry(stringValue);
      storage.getConfiguration().update();
      break;

    case LOCALELANGUAGE:
      storage.getConfiguration().setLocaleLanguage(stringValue);
      storage.getConfiguration().update();
      break;

    case CHARSET:
      storage.getConfiguration().setCharset(stringValue);
      storage.getConfiguration().update();
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
      storage.getConfiguration().setClusterSelection(stringValue);
      storage.getConfiguration().update();
      break;

    case MINIMUMCLUSTERS:
      if (iValue != null) {
        if (iValue instanceof Number)
          storage.getConfiguration().setMinimumClusters(((Number) iValue).intValue());
        else
          storage.getConfiguration().setMinimumClusters(Integer.parseInt(stringValue));
      } else
        // DEFAULT = 1
        storage.getConfiguration().setMinimumClusters(1);

      storage.getConfiguration().update();
      break;

    case CONFLICTSTRATEGY:
      storage.setConflictStrategy(Orient.instance().getRecordConflictStrategy().getStrategy(stringValue));
      storage.getConfiguration().setConflictStrategy(stringValue);
      storage.getConfiguration().update();
      break;

    case VALIDATION:
      storage.getConfiguration().setValidation(Boolean.parseBoolean(stringValue));
      storage.getConfiguration().update();
      break;

    default:
      throw new IllegalArgumentException("Option '" + iAttribute + "' not supported on alter database");

    }

    return (DB) this;
  }

  @Override
  public ORecordMetadata getRecordMetadata(final ORID rid) {
    checkIfActive();
    return storage.getRecordMetadata(rid);
  }

  public OTransaction getTransaction() {
    checkIfActive();
    return currentTx;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecord> RET load(final ORecord iRecord, final String iFetchPlan) {
    checkIfActive();
    return (RET) currentTx.loadRecord(iRecord.getIdentity(), iRecord, iFetchPlan, false, false, OStorage.LOCKING_STRATEGY.DEFAULT);
  }

  @SuppressWarnings("unchecked")
  @Override
  @Deprecated
  public <RET extends ORecord> RET load(ORecord iRecord, String iFetchPlan, boolean iIgnoreCache, boolean loadTombstone,
      OStorage.LOCKING_STRATEGY iLockingStrategy) {
    checkIfActive();
    return (RET) currentTx
        .loadRecord(iRecord.getIdentity(), iRecord, iFetchPlan, iIgnoreCache, !iIgnoreCache, loadTombstone, iLockingStrategy);
  }

  @SuppressWarnings("unchecked")
  @Override
  @Deprecated
  public <RET extends ORecord> RET load(final ORecord iRecord, final String iFetchPlan, final boolean iIgnoreCache,
      final boolean iUpdateCache, final boolean loadTombstone, final OStorage.LOCKING_STRATEGY iLockingStrategy) {
    checkIfActive();
    return (RET) currentTx
        .loadRecord(iRecord.getIdentity(), iRecord, iFetchPlan, iIgnoreCache, iUpdateCache, loadTombstone, iLockingStrategy);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecord> RET load(final ORecord iRecord) {
    checkIfActive();
    return (RET) currentTx.loadRecord(iRecord.getIdentity(), iRecord, null, false);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecord> RET load(final ORID recordId) {
    return (RET) currentTx.loadRecord(recordId, null, null, false);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecord> RET load(final ORID iRecordId, final String iFetchPlan) {
    checkIfActive();
    return (RET) currentTx.loadRecord(iRecordId, null, iFetchPlan, false);
  }

  @SuppressWarnings("unchecked")
  public <RET extends ORecord> RET loadIfVersionIsNotLatest(final ORID rid, final int recordVersion, String fetchPlan,
      boolean ignoreCache) throws ORecordNotFoundException {
    checkIfActive();
    return (RET) currentTx.loadRecordIfVersionIsNotLatest(rid, recordVersion, fetchPlan, ignoreCache);
  }

  @SuppressWarnings("unchecked")
  @Override
  @Deprecated
  public <RET extends ORecord> RET load(final ORID iRecordId, String iFetchPlan, final boolean iIgnoreCache,
      final boolean loadTombstone, OStorage.LOCKING_STRATEGY iLockingStrategy) {
    checkIfActive();
    return (RET) currentTx.loadRecord(iRecordId, null, iFetchPlan, iIgnoreCache, loadTombstone, iLockingStrategy);
  }

  @SuppressWarnings("unchecked")
  @Override
  @Deprecated
  public <RET extends ORecord> RET load(final ORID iRecordId, String iFetchPlan, final boolean iIgnoreCache,
      final boolean iUpdateCache, final boolean loadTombstone, OStorage.LOCKING_STRATEGY iLockingStrategy) {
    checkIfActive();
    return (RET) currentTx.loadRecord(iRecordId, null, iFetchPlan, iIgnoreCache, iUpdateCache, loadTombstone, iLockingStrategy);
  }

  @SuppressWarnings("unchecked")
  public <RET extends ORecord> RET reload(final ORecord iRecord) {
    return reload(iRecord, null, false);
  }

  @SuppressWarnings("unchecked")
  public <RET extends ORecord> RET reload(final ORecord iRecord, final String iFetchPlan) {
    return reload(iRecord, iFetchPlan, false);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <RET extends ORecord> RET reload(final ORecord iRecord, final String iFetchPlan, final boolean iIgnoreCache) {
    return reload(iRecord, iFetchPlan, iIgnoreCache, true);
  }

  @Override
  public <RET extends ORecord> RET reload(final ORecord record, String fetchPlan, boolean ignoreCache, boolean force) {
    checkIfActive();

    final ORecord loadedRecord = currentTx.reloadRecord(record.getIdentity(), record, fetchPlan, ignoreCache, force);

    if (loadedRecord != null && record != loadedRecord) {
      record.fromStream(loadedRecord.toStream());
      ORecordInternal.setVersion(record, loadedRecord.getVersion());
    } else if (loadedRecord == null) {
      throw new ORecordNotFoundException(record.getIdentity());
    }

    return (RET) record;
  }

  /**
   * Deletes the record without checking the version.
   */
  public ODatabaseDocument delete(final ORID iRecord) {
    checkOpeness();
    checkIfActive();

    final ORecord rec = iRecord.getRecord();
    if (rec != null)
      rec.delete();
    return this;
  }

  @Override
  public boolean hide(ORID rid) {
    checkOpeness();
    checkIfActive();

    if (currentTx.isActive())
      throw new ODatabaseException("This operation can be executed only in non transaction mode");

    return executeHideRecord(rid, OPERATION_MODE.SYNCHRONOUS);
  }

  @Override
  public OBinarySerializerFactory getSerializerFactory() {
    return componentsFactory.binarySerializerFactory;
  }

  public ODatabaseDocument begin(final OTransaction iTx) {
    checkOpeness();
    checkIfActive();

    if (currentTx.isActive() && iTx.equals(currentTx)) {
      currentTx.begin();
      return this;
    }

    currentTx.rollback(true, 0);

    // WAKE UP LISTENERS
    for (ODatabaseListener listener : browseListeners())
      try {
        listener.onBeforeTxBegin(this);
      } catch (Exception e) {
        final String message = "Error before the transaction begin";

        OLogManager.instance().error(this, message, e);
        throw OException.wrapException(new OTransactionBlockedException(message), e);
      }

    currentTx = iTx;
    currentTx.begin();

    return this;
  }

  /**
   * {@inheritDoc}
   */
  public <RET extends ORecord> RET load(final ORecord iRecord, final String iFetchPlan, final boolean iIgnoreCache) {
    return (RET) executeReadRecord((ORecordId) iRecord.getIdentity(), iRecord, -1, iFetchPlan, iIgnoreCache, !iIgnoreCache, false,
        OStorage.LOCKING_STRATEGY.NONE, new SimpleRecordReader(prefetchRecords));
  }

  /**
   * This method is internal, it can be subject to signature change or be removed, do not use.
   *
   * @Internal
   */
  public Set<ORecord> executeReadRecords(final Set<ORecordId> iRids, final boolean ignoreCache) {
    checkOpeness();
    checkIfActive();

    getMetadata().makeThreadLocalSchemaSnapshot();
    ORecordSerializationContext.pushContext();
    try {

      final Set<ORecord> records = new HashSet<ORecord>(iRids.size() > 0 ? iRids.size() : 1);

      if (iRids.isEmpty())
        return records;

      final Collection<ORecordId> rids = new ArrayList<ORecordId>(iRids);

      for (Iterator<ORecordId> it = rids.iterator(); it.hasNext(); ) {
        final ORecordId rid = it.next();

        // SEARCH IN LOCAL TX
        ORecord record = getTransaction().getRecord(rid);
        if (record == OTransactionRealAbstract.DELETED_RECORD) {
          // DELETED IN TX
          it.remove();
          continue;
        }

        if (record == null && !ignoreCache)
          // SEARCH INTO THE CACHE
          record = getLocalCache().findRecord(rid);

        if (record != null) {
          // FOUND FROM CACHE
          records.add(record);
          it.remove();
        }
      }

      final Collection<OPair<ORecordId, ORawBuffer>> rawRecords = ((OAbstractPaginatedStorage) storage.getUnderlying())
          .readRecords(rids);
      for (OPair<ORecordId, ORawBuffer> entry : rawRecords) {
        // NO SAME RECORD TYPE: CAN'T REUSE OLD ONE BUT CREATE A NEW ONE FOR IT
        final ORecord record = Orient.instance().getRecordFactoryManager().newInstance(entry.value.recordType);
        ORecordInternal.fill(record, entry.key, entry.value.version, entry.value.buffer, false);
        records.add(record);
      }

      return records;

    } finally {
      ORecordSerializationContext.pullContext();
      getMetadata().clearThreadLocalSchemaSnapshot();
    }
  }

  /**
   * This method is internal, it can be subject to signature change or be removed, do not use.
   *
   * @Internal
   */
  public <RET extends ORecord> RET executeReadRecord(final ORecordId rid, ORecord iRecord, final int recordVersion,
      final String fetchPlan, final boolean ignoreCache, final boolean iUpdateCache, final boolean loadTombstones,
      final OStorage.LOCKING_STRATEGY lockingStrategy, RecordReader recordReader) {
    checkOpeness();
    checkIfActive();

    getMetadata().makeThreadLocalSchemaSnapshot();
    ORecordSerializationContext.pushContext();
    try {
      checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, getClusterNameById(rid.getClusterId()));

      // SEARCH IN LOCAL TX
      ORecord record = getTransaction().getRecord(rid);
      if (record == OTransactionRealAbstract.DELETED_RECORD)
        // DELETED IN TX
        return null;

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
        if (callbackHooks(ORecordHook.TYPE.BEFORE_READ, record) == ORecordHook.RESULT.SKIP)
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

        callbackHooks(ORecordHook.TYPE.AFTER_READ, record);
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

        recordBuffer = recordReader.readRecord(storage, rid, fetchPlan, ignoreCache, version);
      }

      if (recordBuffer == null)
        return null;

      if (iRecord == null || ORecordInternal.getRecordType(iRecord) != recordBuffer.recordType)
        // NO SAME RECORD TYPE: CAN'T REUSE OLD ONE BUT CREATE A NEW ONE FOR IT
        iRecord = Orient.instance().getRecordFactoryManager().newInstance(recordBuffer.recordType);

      ORecordInternal.fill(iRecord, rid, recordBuffer.version, recordBuffer.buffer, false);

      if (iRecord instanceof ODocument)
        ODocumentInternal.checkClass((ODocument) iRecord, this);

      if (ORecordVersionHelper.isTombstone(iRecord.getVersion()))
        return (RET) iRecord;

      if (callbackHooks(ORecordHook.TYPE.BEFORE_READ, iRecord) == ORecordHook.RESULT.SKIP)
        return null;

      iRecord.fromStream(recordBuffer.buffer);

      callbackHooks(ORecordHook.TYPE.AFTER_READ, iRecord);

      if (iUpdateCache)
        getLocalCache().updateRecord(iRecord);

      return (RET) iRecord;
    } catch (OOfflineClusterException t) {
      throw t;
    } catch (ORecordNotFoundException t) {
      throw t;
    } catch (Throwable t) {
      if (rid.isTemporary())
        throw OException.wrapException(new ODatabaseException("Error on retrieving record using temporary RID: " + rid), t);
      else
        throw OException.wrapException(new ODatabaseException(
            "Error on retrieving record " + rid + " (cluster: " + storage.getPhysicalClusterNameById(rid.getClusterId()) + ")"), t);
    } finally {
      ORecordSerializationContext.pullContext();
      getMetadata().clearThreadLocalSchemaSnapshot();
    }
  }

  public int assignAndCheckCluster(ORecord record, String iClusterName) {
    ORecordId rid = (ORecordId) record.getIdentity();
    // if provided a cluster name use it.
    if (rid.getClusterId() <= ORID.CLUSTER_POS_INVALID && iClusterName != null) {
      rid.setClusterId(getClusterIdByName(iClusterName));
      if (rid.getClusterId() == -1)
        throw new IllegalArgumentException("Cluster name '" + iClusterName + "' is not configured");

    }
    OClass schemaClass = null;
    // if cluster id is not set yet try to find it out
    if (rid.getClusterId() <= ORID.CLUSTER_ID_INVALID && storage.isAssigningClusterIds()) {
      if (record instanceof ODocument) {
        schemaClass = ODocumentInternal.getImmutableSchemaClass(((ODocument) record));
        if (schemaClass != null) {
          if (schemaClass.isAbstract())
            throw new OSchemaException("Document belongs to abstract class " + schemaClass.getName() + " and cannot be saved");
          rid.setClusterId(schemaClass.getClusterForNewInstance((ODocument) record));
        } else
          rid.setClusterId(getDefaultClusterId());
      } else {
        rid.setClusterId(getDefaultClusterId());
        if (record instanceof OBlob && rid.getClusterId() != ORID.CLUSTER_ID_INVALID) {
          // Set<Integer> blobClusters = getMetadata().getSchema().getBlobClusters();
          // if (!blobClusters.contains(rid.clusterId) && rid.clusterId != getDefaultClusterId() && rid.clusterId != 0) {
          // if (iClusterName == null)
          // iClusterName = getClusterNameById(rid.clusterId);
          // throw new IllegalArgumentException(
          // "Cluster name '" + iClusterName + "' (id=" + rid.clusterId + ") is not configured to store blobs, valid are "
          // + blobClusters.toString());
          // }
        }
      }
    } else if (record instanceof ODocument)
      schemaClass = ODocumentInternal.getImmutableSchemaClass(((ODocument) record));
    // If the cluster id was set check is validity
    if (rid.getClusterId() > ORID.CLUSTER_ID_INVALID) {
      if (schemaClass != null) {
        String messageClusterName = getClusterNameById(rid.getClusterId());
        checkRecordClass(schemaClass, messageClusterName, rid);
        if (!schemaClass.hasClusterId(rid.getClusterId())) {
          throw new IllegalArgumentException(
              "Cluster name '" + messageClusterName + "' (id=" + rid.getClusterId() + ") is not configured to store the class '"
                  + schemaClass.getName() + "', valid are " + Arrays.toString(schemaClass.getClusterIds()));
        }
      }
    }
    return rid.getClusterId();
  }

  public <RET extends ORecord> RET executeSaveEmptyRecord(ORecord record, String clusterName) {
    ORecordId rid = (ORecordId) record.getIdentity();
    assert rid.isNew();

    ORecordInternal.onBeforeIdentityChanged(record);
    int id = assignAndCheckCluster(record, clusterName);
    clusterName = getClusterNameById(id);
    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_CREATE, clusterName);

    byte[] content = getSerializer().writeClassOnly(record);

    final OStorageOperationResult<OPhysicalPosition> ppos = storage
        .createRecord(rid, content, record.getVersion(), recordType, OPERATION_MODE.SYNCHRONOUS.ordinal(), null);

    ORecordInternal.setVersion(record, ppos.getResult().recordVersion);
    ((ORecordId) record.getIdentity()).copyFrom(rid);
    ORecordInternal.onAfterIdentityChanged(record);

    return (RET) record;
  }

  /**
   * This method is internal, it can be subject to signature change or be removed, do not use.
   *
   * @Internal
   */
  public <RET extends ORecord> RET executeSaveRecord(final ORecord record, String clusterName, final int ver,
      final OPERATION_MODE mode, boolean forceCreate, final ORecordCallback<? extends Number> recordCreatedCallback,
      ORecordCallback<Integer> recordUpdatedCallback) {
    checkOpeness();
    checkIfActive();
    if (!record.isDirty())
      return (RET) record;

    final ORecordId rid = (ORecordId) record.getIdentity();

    if (rid == null)
      throw new ODatabaseException(
          "Cannot create record because it has no identity. Probably is not a regular record or contains projections of fields rather than a full record");

    record.setInternalStatus(ORecordElement.STATUS.MARSHALLING);
    try {

      byte[] stream = null;
      final OStorageOperationResult<Integer> operationResult;

      getMetadata().makeThreadLocalSchemaSnapshot();
      if (record instanceof ODocument)
        ODocumentInternal.checkClass((ODocument) record, this);
      ORecordSerializationContext.pushContext();
      final boolean isNew = forceCreate || rid.isNew();
      try {

        final ORecordHook.TYPE triggerType;
        if (isNew) {
          // NOTIFY IDENTITY HAS CHANGED
          ORecordInternal.onBeforeIdentityChanged(record);
          int id = assignAndCheckCluster(record, clusterName);
          clusterName = getClusterNameById(id);
          checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_CREATE, clusterName);
          triggerType = ORecordHook.TYPE.BEFORE_CREATE;
        } else {
          clusterName = getClusterNameById(record.getIdentity().getClusterId());
          checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_UPDATE, clusterName);
          triggerType = ORecordHook.TYPE.BEFORE_UPDATE;
        }
        stream = record.toStream();

        final ORecordHook.RESULT hookResult = callbackHooks(triggerType, record);

        if (hookResult == ORecordHook.RESULT.RECORD_CHANGED) {
          if (record instanceof ODocument)
            ((ODocument) record).validate();
          stream = updateStream(record);
        } else if (hookResult == ORecordHook.RESULT.SKIP_IO)
          return (RET) record;
        else if (hookResult == ORecordHook.RESULT.RECORD_REPLACED)
          // RETURNED THE REPLACED RECORD
          return (RET) OHookReplacedRecordThreadLocal.INSTANCE.get();

        ORecordSaveThreadLocal.setLast(record);
        try {
          // SAVE IT
          boolean updateContent = ORecordInternal.isContentChanged(record);
          byte[] content = (stream == null) ? OCommonConst.EMPTY_BYTE_ARRAY : stream;
          byte recordType = ORecordInternal.getRecordType(record);
          final int modeIndex = mode.ordinal();

          // CHECK IF RECORD TYPE IS SUPPORTED
          Orient.instance().getRecordFactoryManager().getRecordTypeClass(recordType);

          if (forceCreate || ORecordId.isNew(rid.getClusterPosition())) {
            // CREATE
            final OStorageOperationResult<OPhysicalPosition> ppos = storage
                .createRecord(rid, content, ver, recordType, modeIndex, (ORecordCallback<Long>) recordCreatedCallback);
            operationResult = new OStorageOperationResult<Integer>(ppos.getResult().recordVersion, ppos.isMoved());

          } else {
            // UPDATE
            operationResult = storage.updateRecord(rid, updateContent, content, ver, recordType, modeIndex, recordUpdatedCallback);
          }

          final int version = operationResult.getResult();

          if (isNew) {
            // UPDATE INFORMATION: CLUSTER ID+POSITION
            ((ORecordId) record.getIdentity()).copyFrom(rid);
            // NOTIFY IDENTITY HAS CHANGED
            ORecordInternal.onAfterIdentityChanged(record);
            // UPDATE INFORMATION: CLUSTER ID+POSITION
          }

          if (operationResult.getModifiedRecordContent() != null)
            stream = operationResult.getModifiedRecordContent();
          else if (version > record.getVersion() + 1)
            // IN CASE OF REMOTE CONFLICT STRATEGY FORCE UNLOAD DUE TO INVALID CONTENT
            record.unload();

          ORecordInternal.fill(record, rid, version, stream, false);

          callbackHookSuccess(record, isNew, stream, operationResult);
        } catch (Exception t) {
          callbackHookFailure(record, isNew, stream);
          throw t;
        }
      } finally {
        callbackHookFinalize(record, isNew, stream);
        ORecordSerializationContext.pullContext();
        getMetadata().clearThreadLocalSchemaSnapshot();
        ORecordSaveThreadLocal.removeLast();
      }

      if (stream != null && stream.length > 0 && !operationResult.isMoved())
        // ADD/UPDATE IT IN CACHE IF IT'S ACTIVE
        getLocalCache().updateRecord(record);
    } catch (OException e) {
      throw e;
    } catch (Exception t) {
      if (!ORecordId.isValid(record.getIdentity().getClusterPosition()))
        throw OException
            .wrapException(new ODatabaseException("Error on saving record in cluster #" + record.getIdentity().getClusterId()), t);
      else
        throw OException.wrapException(new ODatabaseException("Error on saving record " + record.getIdentity()), t);

    } finally {
      record.setInternalStatus(ORecordElement.STATUS.LOADED);
    }
    return (RET) record;
  }

  /**
   * This method is internal, it can be subject to signature change or be removed, do not use.
   *
   * @Internal
   */
  public void executeDeleteRecord(OIdentifiable record, final int iVersion, final boolean iRequired, final OPERATION_MODE iMode,
      boolean prohibitTombstones) {
    checkOpeness();
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

    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_DELETE, getClusterNameById(rid.getClusterId()));

    ORecordSerializationContext.pushContext();
    getMetadata().makeThreadLocalSchemaSnapshot();
    try {
      if (record instanceof ODocument) {
        ODocumentInternal.checkClass((ODocument) record, this);
      }
      try {
        // if cache is switched off record will be unreachable after delete.
        ORecord rec = record.getRecord();
        if (rec != null) {
          callbackHooks(ORecordHook.TYPE.BEFORE_DELETE, rec);

          if (rec instanceof ODocument)
            ORidBagDeleter.deleteAllRidBags((ODocument) rec);
        }

        final OStorageOperationResult<Boolean> operationResult;
        try {
          if (prohibitTombstones) {
            final boolean result = storage.cleanOutRecord(rid, iVersion, iMode.ordinal(), null);
            if (!result && iRequired)
              throw new ORecordNotFoundException(rid);
            operationResult = new OStorageOperationResult<Boolean>(result);
          } else {
            final OStorageOperationResult<Boolean> result = storage.deleteRecord(rid, iVersion, iMode.ordinal(), null);
            if (!result.getResult() && iRequired)
              throw new ORecordNotFoundException(rid);
            operationResult = new OStorageOperationResult<Boolean>(result.getResult());
          }

          if (!operationResult.isMoved() && rec != null)
            callbackHooks(ORecordHook.TYPE.AFTER_DELETE, rec);
          else if (rec != null)
            callbackHooks(ORecordHook.TYPE.DELETE_REPLICATED, rec);
        } catch (Exception t) {
          callbackHooks(ORecordHook.TYPE.DELETE_FAILED, rec);
          throw t;
        } finally {
          callbackHooks(ORecordHook.TYPE.FINALIZE_DELETION, rec);
        }

        clearDocumentTracking(rec);

        // REMOVE THE RECORD FROM 1 AND 2 LEVEL CACHES
        if (!operationResult.isMoved()) {
          getLocalCache().deleteRecord(rid);
        }

      } catch (OException e) {
        // RE-THROW THE EXCEPTION
        throw e;

      } catch (Exception t) {
        // WRAP IT AS ODATABASE EXCEPTION
        throw OException
            .wrapException(new ODatabaseException("Error on deleting record in cluster #" + record.getIdentity().getClusterId()),
                t);
      }
    } finally {
      ORecordSerializationContext.pullContext();
      getMetadata().clearThreadLocalSchemaSnapshot();
    }
  }

  /**
   * This method is internal, it can be subject to signature change or be removed, do not use.
   *
   * @Internal
   */
  public boolean executeHideRecord(OIdentifiable record, final OPERATION_MODE iMode) {
    checkOpeness();
    checkIfActive();

    final ORecordId rid = (ORecordId) record.getIdentity();

    if (rid == null)
      throw new ODatabaseException(
          "Cannot hide record because it has no identity. Probably was created from scratch or contains projections of fields rather than a full record");

    if (!rid.isValid())
      return false;

    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_DELETE, getClusterNameById(rid.getClusterId()));

    getMetadata().makeThreadLocalSchemaSnapshot();
    if (record instanceof ODocument)
      ODocumentInternal.checkClass((ODocument) record, this);
    ORecordSerializationContext.pushContext();
    try {

      final OStorageOperationResult<Boolean> operationResult;
      operationResult = storage.hideRecord(rid, iMode.ordinal(), null);

      // REMOVE THE RECORD FROM 1 AND 2 LEVEL CACHES
      if (!operationResult.isMoved())
        getLocalCache().deleteRecord(rid);

      return operationResult.getResult();
    } finally {
      ORecordSerializationContext.pullContext();
      getMetadata().clearThreadLocalSchemaSnapshot();
    }
  }

  public ODatabaseDocumentTx begin() {
    return begin(OTransaction.TXTYPE.OPTIMISTIC);
  }

  public ODatabaseDocumentTx begin(final OTransaction.TXTYPE iType) {
    checkOpeness();
    checkIfActive();

    if (currentTx.isActive()) {
      if (iType == OTransaction.TXTYPE.OPTIMISTIC && currentTx instanceof OTransactionOptimistic) {
        currentTx.begin();
        return this;
      }

      currentTx.rollback(true, 0);
    }

    // CHECK IT'S NOT INSIDE A HOOK
    if (!inHook.isEmpty())
      throw new IllegalStateException("Cannot begin a transaction while a hook is executing");

    // WAKE UP LISTENERS
    for (ODatabaseListener listener : browseListeners())
      try {
        listener.onBeforeTxBegin(this);
      } catch (Throwable t) {
        OLogManager.instance().error(this, "Error before tx begin", t);
      }

    switch (iType) {
    case NOTX:
      setDefaultTransactionMode();
      break;

    case OPTIMISTIC:
      currentTx = new OTransactionOptimistic(this);
      break;

    case PESSIMISTIC:
      throw new UnsupportedOperationException("Pessimistic transaction");
    }

    currentTx.begin();
    return this;
  }

  public void setDefaultTransactionMode() {
    if (!(currentTx instanceof OTransactionNoTx))
      currentTx = new OTransactionNoTx(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void freeze(final boolean throwException) {
    checkOpeness();
    if (!(getStorage() instanceof OFreezableStorageComponent)) {
      OLogManager.instance().error(this,
          "Only local paginated storage supports freeze. If you are using remote client please use OServerAdmin instead");

      return;
    }

    final long startTime = Orient.instance().getProfiler().startChrono();

    final OFreezableStorageComponent storage = getFreezableStorage();
    if (storage != null) {
      storage.freeze(throwException);
    }

    Orient.instance().getProfiler()
        .stopChrono("db." + getName() + ".freeze", "Time to freeze the database", startTime, "db.*.freeze");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void freeze() {
    checkOpeness();
    if (!(getStorage() instanceof OFreezableStorageComponent)) {
      OLogManager.instance().error(this,
          "Only local paginated storage supports freeze. " + "If you use remote client please use OServerAdmin instead");

      return;
    }

    final long startTime = Orient.instance().getProfiler().startChrono();

    final OFreezableStorageComponent storage = getFreezableStorage();
    if (storage != null) {
      storage.freeze(false);
    }

    Orient.instance().getProfiler()
        .stopChrono("db." + getName() + ".freeze", "Time to freeze the database", startTime, "db.*.freeze");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void release() {
    checkOpeness();
    if (!(getStorage() instanceof OFreezableStorageComponent)) {
      OLogManager.instance().error(this,
          "Only local paginated storage supports release. If you are using remote client please use OServerAdmin instead");
      return;
    }

    final long startTime = Orient.instance().getProfiler().startChrono();

    final OFreezableStorageComponent storage = getFreezableStorage();
    if (storage != null) {
      storage.release();
    }

    Orient.instance().getProfiler()
        .stopChrono("db." + getName() + ".release", "Time to release the database", startTime, "db.*.release");
  }

  /**
   * Creates a new ODocument.
   */
  public ODocument newInstance() {
    return new ODocument();
  }

  /**
   * Creates a document with specific class.
   *
   * @param iClassName the name of class that should be used as a class of created document.
   *
   * @return new instance of document.
   */
  @Override
  public ODocument newInstance(final String iClassName) {
    return new ODocument(iClassName);
  }

  /**
   * {@inheritDoc}
   */
  public ORecordIteratorClass<ODocument> browseClass(final String iClassName) {
    return browseClass(iClassName, true);
  }

  /**
   * {@inheritDoc}
   */
  public ORecordIteratorClass<ODocument> browseClass(final String iClassName, final boolean iPolymorphic) {
    if (getMetadata().getImmutableSchemaSnapshot().getClass(iClassName) == null)
      throw new IllegalArgumentException("Class '" + iClassName + "' not found in current database");

    checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_READ, iClassName);
    return new ORecordIteratorClass<ODocument>(this, this, iClassName, iPolymorphic, false);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ORecordIteratorCluster<ODocument> browseCluster(final String iClusterName) {
    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, iClusterName);

    return new ORecordIteratorCluster<ODocument>(this, this, getClusterIdByName(iClusterName));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterable<ODatabaseListener> getListeners() {
    return getListenersCopy();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @Deprecated
  public ORecordIteratorCluster<ODocument> browseCluster(String iClusterName, long startClusterPosition, long endClusterPosition,
      boolean loadTombstones) {
    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, iClusterName);

    return new ORecordIteratorCluster<ODocument>(this, this, getClusterIdByName(iClusterName), startClusterPosition,
        endClusterPosition, loadTombstones, OStorage.LOCKING_STRATEGY.DEFAULT);
  }

  /**
   * Saves a document to the database. Behavior depends by the current running transaction if any. If no transaction is running then
   * changes apply immediately. If an Optimistic transaction is running then the record will be changed at commit time. The current
   * transaction will continue to see the record as modified, while others not. If a Pessimistic transaction is running, then an
   * exclusive lock is acquired against the record. Current transaction will continue to see the record as modified, while others
   * cannot access to it since it's locked.
   * <p/>
   * If MVCC is enabled and the version of the document is different by the version stored in the database, then a
   * {@link OConcurrentModificationException} exception is thrown.Before to save the document it must be valid following the
   * constraints declared in the schema if any (can work also in schema-less mode). To validate the document the
   * {@link ODocument#validate()} is called.
   *
   * @param iRecord Record to save.
   *
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   *
   * @throws OConcurrentModificationException if the version of the document is different by the version contained in the database.
   * @throws OValidationException             if the document breaks some validation constraints defined in the schema
   * @see #setMVCC(boolean), {@link #isMVCC()}
   */
  @Override
  public <RET extends ORecord> RET save(final ORecord iRecord) {
    return (RET) save(iRecord, null, OPERATION_MODE.SYNCHRONOUS, false, null, null);
  }

  /**
   * Saves a document to the database. Behavior depends by the current running transaction if any. If no transaction is running then
   * changes apply immediately. If an Optimistic transaction is running then the record will be changed at commit time. The current
   * transaction will continue to see the record as modified, while others not. If a Pessimistic transaction is running, then an
   * exclusive lock is acquired against the record. Current transaction will continue to see the record as modified, while others
   * cannot access to it since it's locked.
   * <p/>
   * If MVCC is enabled and the version of the document is different by the version stored in the database, then a
   * {@link OConcurrentModificationException} exception is thrown.Before to save the document it must be valid following the
   * constraints declared in the schema if any (can work also in schema-less mode). To validate the document the
   * {@link ODocument#validate()} is called.
   *
   * @param iRecord                Record to save.
   * @param iForceCreate           Flag that indicates that record should be created. If record with current rid already exists,
   *                               exception is thrown
   * @param iRecordCreatedCallback callback that is called after creation of new record
   * @param iRecordUpdatedCallback callback that is called after record update
   *
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   *
   * @throws OConcurrentModificationException if the version of the document is different by the version contained in the database.
   * @throws OValidationException             if the document breaks some validation constraints defined in the schema
   * @see #setMVCC(boolean), {@link #isMVCC()}
   */
  @Override
  public <RET extends ORecord> RET save(final ORecord iRecord, final OPERATION_MODE iMode, boolean iForceCreate,
      final ORecordCallback<? extends Number> iRecordCreatedCallback, ORecordCallback<Integer> iRecordUpdatedCallback) {
    return save(iRecord, null, iMode, iForceCreate, iRecordCreatedCallback, iRecordUpdatedCallback);
  }

  /**
   * Saves a document specifying a cluster where to store the record. Behavior depends by the current running transaction if any. If
   * no transaction is running then changes apply immediately. If an Optimistic transaction is running then the record will be
   * changed at commit time. The current transaction will continue to see the record as modified, while others not. If a Pessimistic
   * transaction is running, then an exclusive lock is acquired against the record. Current transaction will continue to see the
   * record as modified, while others cannot access to it since it's locked.
   * <p/>
   * If MVCC is enabled and the version of the document is different by the version stored in the database, then a
   * {@link OConcurrentModificationException} exception is thrown. Before to save the document it must be valid following the
   * constraints declared in the schema if any (can work also in schema-less mode). To validate the document the
   * {@link ODocument#validate()} is called.
   *
   * @param iRecord      Record to save
   * @param iClusterName Cluster name where to save the record
   *
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   *
   * @throws OConcurrentModificationException if the version of the document is different by the version contained in the database.
   * @throws OValidationException             if the document breaks some validation constraints defined in the schema
   * @see #setMVCC(boolean), {@link #isMVCC()}, ODocument#validate()
   */
  @Override
  public <RET extends ORecord> RET save(final ORecord iRecord, final String iClusterName) {
    return (RET) save(iRecord, iClusterName, OPERATION_MODE.SYNCHRONOUS, false, null, null);
  }

  /**
   * Saves a document specifying a cluster where to store the record. Behavior depends by the current running transaction if any. If
   * no transaction is running then changes apply immediately. If an Optimistic transaction is running then the record will be
   * changed at commit time. The current transaction will continue to see the record as modified, while others not. If a Pessimistic
   * transaction is running, then an exclusive lock is acquired against the record. Current transaction will continue to see the
   * record as modified, while others cannot access to it since it's locked.
   * <p/>
   * If MVCC is enabled and the version of the document is different by the version stored in the database, then a
   * {@link OConcurrentModificationException} exception is thrown. Before to save the document it must be valid following the
   * constraints declared in the schema if any (can work also in schema-less mode). To validate the document the
   * {@link ODocument#validate()} is called.
   *
   * @param iRecord                Record to save
   * @param iClusterName           Cluster name where to save the record
   * @param iMode                  Mode of save: synchronous (default) or asynchronous
   * @param iForceCreate           Flag that indicates that record should be created. If record with current rid already exists,
   *                               exception is thrown
   * @param iRecordCreatedCallback callback that is called after creation of new record
   * @param iRecordUpdatedCallback callback that is called after record update
   *
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   *
   * @throws OConcurrentModificationException if the version of the document is different by the version contained in the database.
   * @throws OValidationException             if the document breaks some validation constraints defined in the schema
   * @see #setMVCC(boolean), {@link #isMVCC()}, ODocument#validate()
   */
  @Override
  public <RET extends ORecord> RET save(final ORecord iRecord, String iClusterName, final OPERATION_MODE iMode,
      boolean iForceCreate, final ORecordCallback<? extends Number> iRecordCreatedCallback,
      ORecordCallback<Integer> iRecordUpdatedCallback) {
    checkOpeness();

    if (!(iRecord instanceof ODocument)) {
      assignAndCheckCluster(iRecord, iClusterName);
      return (RET) currentTx.saveRecord(iRecord, iClusterName, iMode, iForceCreate, iRecordCreatedCallback, iRecordUpdatedCallback);
    }

    ODocument doc = (ODocument) iRecord;
    ODocumentInternal.checkClass(doc, this);
    // IN TX THE VALIDATION MAY BE RUN TWICE BUT IS CORRECT BECAUSE OF DIFFERENT RECORD STATUS
    try {
      doc.validate();
    } catch (OValidationException ex) {
      doc.undo();
      throw ex;
    }
    ODocumentInternal.convertAllMultiValuesToTrackedVersions(doc);

    if (iForceCreate || !doc.getIdentity().isValid()) {
      if (doc.getClassName() != null)
        checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_CREATE, doc.getClassName());

      assignAndCheckCluster(doc, iClusterName);

    } else {
      // UPDATE: CHECK ACCESS ON SCHEMA CLASS NAME (IF ANY)
      if (doc.getClassName() != null)
        checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_UPDATE, doc.getClassName());
    }

    doc = (ODocument) currentTx
        .saveRecord(iRecord, iClusterName, iMode, iForceCreate, iRecordCreatedCallback, iRecordUpdatedCallback);

    return (RET) doc;
  }

  /**
   * Deletes a document. Behavior depends by the current running transaction if any. If no transaction is running then the record is
   * deleted immediately. If an Optimistic transaction is running then the record will be deleted at commit time. The current
   * transaction will continue to see the record as deleted, while others not. If a Pessimistic transaction is running, then an
   * exclusive lock is acquired against the record. Current transaction will continue to see the record as deleted, while others
   * cannot access to it since it's locked.
   * <p/>
   * If MVCC is enabled and the version of the document is different by the version stored in the database, then a
   * {@link OConcurrentModificationException} exception is thrown.
   *
   * @param record record to delete
   *
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   *
   * @see #setMVCC(boolean), {@link #isMVCC()}
   */
  public ODatabaseDocumentTx delete(final ORecord record) {
    checkOpeness();
    if (record == null)
      throw new ODatabaseException("Cannot delete null document");

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

  /**
   * Returns the number of the records of the class iClassName.
   */
  public long countClass(final String iClassName) {
    return countClass(iClassName, true);
  }

  /**
   * Returns the number of the records of the class iClassName considering also sub classes if polymorphic is true.
   */
  public long countClass(final String iClassName, final boolean iPolymorphic) {
    final OClass cls = getMetadata().getImmutableSchemaSnapshot().getClass(iClassName);

    if (cls == null)
      throw new IllegalArgumentException("Class '" + iClassName + "' not found in database");

    long totalOnDb = cls.count(iPolymorphic);

    long deletedInTx = 0;
    long addedInTx = 0;
    if (getTransaction().isActive())
      for (ORecordOperation op : getTransaction().getAllRecordEntries()) {
        if (op.type == ORecordOperation.DELETED) {
          final ORecord rec = op.getRecord();
          if (rec != null && rec instanceof ODocument) {
            OClass schemaClass = ((ODocument) rec).getSchemaClass();
            if (iPolymorphic) {
              if (schemaClass.isSubClassOf(iClassName))
                deletedInTx++;
            } else {
              if (iClassName.equals(schemaClass.getName()) || iClassName.equals(schemaClass.getShortName()))
                deletedInTx++;
            }
          }
        }
        if (op.type == ORecordOperation.CREATED) {
          final ORecord rec = op.getRecord();
          if (rec != null && rec instanceof ODocument) {
            OClass schemaClass = ((ODocument) rec).getSchemaClass();
            if (iPolymorphic) {
              if (schemaClass.isSubClassOf(iClassName))
                addedInTx++;
            } else {
              if (iClassName.equals(schemaClass.getName()) || iClassName.equals(schemaClass.getShortName()))
                addedInTx++;
            }
          }
        }
      }

    return (totalOnDb + addedInTx) - deletedInTx;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ODatabase<ORecord> commit() {
    return commit(false);
  }

  @Override
  public ODatabaseDocument commit(boolean force) throws OTransactionException {
    checkOpeness();
    checkIfActive();

    if (!currentTx.isActive())
      return this;

    if (!force && currentTx.amountOfNestedTxs() > 1) {
      currentTx.commit();
      return this;
    }

    // WAKE UP LISTENERS
    for (ODatabaseListener listener : browseListeners())
      try {
        listener.onBeforeTxCommit(this);
      } catch (Exception e) {
        rollback(force);

        OLogManager.instance().error(this, "Cannot commit the transaction: caught exception on execution of %s.onBeforeTxCommit()",
            listener.getClass().getName(), e);
        throw OException.wrapException(new OTransactionException(
            "Cannot commit the transaction: caught exception on execution of " + listener.getClass().getName()
                + "#onBeforeTxCommit()"), e);
      }

    try {
      currentTx.commit(force);
    } catch (RuntimeException e) {
      OLogManager.instance().debug(this, "Error on transaction commit", e);

      // WAKE UP ROLLBACK LISTENERS
      for (ODatabaseListener listener : browseListeners())
        try {
          listener.onBeforeTxRollback(this);
        } catch (Throwable t) {
          OLogManager.instance().error(this, "Error before transaction rollback", t);
        }

      // ROLLBACK TX AT DB LEVEL
      currentTx.rollback(false, 0);
      getLocalCache().clear();

      activateOnCurrentThread();

      // WAKE UP ROLLBACK LISTENERS
      for (ODatabaseListener listener : browseListeners())
        try {
          listener.onAfterTxRollback(this);
        } catch (Throwable t) {
          OLogManager.instance().error(this, "Error after transaction rollback", t);
        }
      throw e;
    }

    // WAKE UP LISTENERS
    for (ODatabaseListener listener : browseListeners())
      try {
        listener.onAfterTxCommit(this);
      } catch (Exception e) {
        final String message =
            "Error after the transaction has been committed. The transaction remains valid. The exception caught was on execution of "
                + listener.getClass() + ".onAfterTxCommit()";

        OLogManager.instance().error(this, message, e);

        throw OException.wrapException(new OTransactionBlockedException(message), e);

      }

    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ODatabase<ORecord> rollback() {
    return rollback(false);
  }

  @Override
  public ODatabaseDocument rollback(boolean force) throws OTransactionException {
    checkOpeness();
    if (currentTx.isActive()) {

      if (!force && currentTx.amountOfNestedTxs() > 1) {
        currentTx.rollback();
        return this;
      }

      // WAKE UP LISTENERS
      for (ODatabaseListener listener : browseListeners())
        try {
          listener.onBeforeTxRollback(this);
        } catch (Throwable t) {
          OLogManager.instance().error(this, "Error before transactional rollback", t);
        }

      currentTx.rollback(force, -1);

      // WAKE UP LISTENERS
      for (ODatabaseListener listener : browseListeners())
        try {
          listener.onAfterTxRollback(this);
        } catch (Throwable t) {
          OLogManager.instance().error(this, "Error after transaction rollback", t);
        }
    }

    getLocalCache().clear();

    return this;
  }

  /**
   * This method is internal, it can be subject to signature change or be removed, do not use.
   *
   * @Internal
   */
  @Override
  public <DB extends ODatabase> DB getUnderlying() {
    throw new UnsupportedOperationException();
  }

  /**
   * This method is internal, it can be subject to signature change or be removed, do not use.
   *
   * @Internal
   */
  @Override
  public OStorage getStorage() {
    return storage;
  }

  /**
   * This method is internal, it can be subject to signature change or be removed, do not use.
   *
   * @Internal
   */
  @Override
  public void replaceStorage(OStorage iNewStorage) {
    storage = iNewStorage;
  }

  @Override
  public <V> V callInLock(final Callable<V> iCallable, final boolean iExclusiveLock) {
    return storage.callInLock(iCallable, iExclusiveLock);
  }

  @Override
  public List<String> backup(final OutputStream out, final Map<String, Object> options, final Callable<Object> callable,
      final OCommandOutputListener iListener, final int compressionLevel, final int bufferSize) throws IOException {
    return storage.backup(out, options, callable, iListener, compressionLevel, bufferSize);
  }

  @Override
  public void restore(final InputStream in, final Map<String, Object> options, final Callable<Object> callable,
      final OCommandOutputListener iListener) throws IOException {
    if (storage == null)
      storage = Orient.instance().loadStorage(url);

    storage.restore(in, options, callable, iListener);

    if (!isClosed())
      getMetadata().reload();
  }

  /**
   * {@inheritDoc}
   */
  public OSBTreeCollectionManager getSbTreeCollectionManager() {
    return getStorage().getSBtreeCollectionManager();
  }

  @Override
  public OCurrentStorageComponentsFactory getStorageVersions() {
    return componentsFactory;
  }

  public ORecordSerializer getSerializer() {
    return serializer;
  }

  /**
   * Sets serializer for the database which will be used for document serialization.
   *
   * @param serializer the serializer to set.
   */
  public void setSerializer(ORecordSerializer serializer) {
    this.serializer = serializer;
  }

  @Override
  public void resetInitialization() {
    for (ORecordHook h : hooks.keySet())
      h.onUnregister();

    hooks.clear();
    compileHooks();

    close();

    initialized = false;
  }

  @Override
  public String incrementalBackup(final String path) {
    checkOpeness();
    checkIfActive();

    return storage.incrementalBackup(path);
  }

  @Override
  @Deprecated
  public <DB extends ODatabaseDocument> DB checkSecurity(final String iResource, final int iOperation) {
    final String resourceSpecific = ORule.mapLegacyResourceToSpecificResource(iResource);
    final ORule.ResourceGeneric resourceGeneric = ORule.mapLegacyResourceToGenericResource(iResource);

    if (resourceSpecific == null || resourceSpecific.equals("*"))
      checkSecurity(resourceGeneric, null, iOperation);

    return checkSecurity(resourceGeneric, resourceSpecific, iOperation);
  }

  @Override
  @Deprecated
  public <DB extends ODatabaseDocument> DB checkSecurity(final String iResourceGeneric, final int iOperation,
      final Object iResourceSpecific) {
    final ORule.ResourceGeneric resourceGeneric = ORule.mapLegacyResourceToGenericResource(iResourceGeneric);
    if (iResourceSpecific == null || iResourceSpecific.equals("*"))
      return checkSecurity(resourceGeneric, iOperation, (Object) null);

    return checkSecurity(resourceGeneric, iOperation, iResourceSpecific);
  }

  @Override
  @Deprecated
  public <DB extends ODatabaseDocument> DB checkSecurity(final String iResourceGeneric, final int iOperation,
      final Object... iResourcesSpecific) {
    final ORule.ResourceGeneric resourceGeneric = ORule.mapLegacyResourceToGenericResource(iResourceGeneric);
    return checkSecurity(resourceGeneric, iOperation, iResourcesSpecific);
  }

  /**
   * @return <code>true</code> if database is obtained from the pool and <code>false</code> otherwise.
   */
  @Override
  public boolean isPooled() {
    return false;
  }

  /**
   * Use #activateOnCurrentThread instead.
   */
  @Deprecated
  public void setCurrentDatabaseInThreadLocal() {
    activateOnCurrentThread();
  }

  /**
   * Activates current database instance on current thread.
   */
  @Override
  public ODatabaseDocumentTx activateOnCurrentThread() {
    final ODatabaseRecordThreadLocal tl = ODatabaseRecordThreadLocal.INSTANCE;
    if (tl != null)
      tl.set(this);
    return this;
  }

  @Override
  public boolean isActiveOnCurrentThread() {
    final ODatabaseRecordThreadLocal tl = ODatabaseRecordThreadLocal.INSTANCE;
    final ODatabaseDocumentInternal db = tl != null ? tl.getIfDefined() : null;
    return db == this;
  }

  protected void checkOpeness() {
    if (isClosed())
      throw new ODatabaseException("Database '" + getURL() + "' is closed");
  }

  private void popInHook(OIdentifiable id) {
    inHook.remove(id);
  }

  private boolean pushInHook(OIdentifiable id) {
    return inHook.add(id);
  }

  private void initAtFirstOpen(String iUserName, String iUserPassword) {
    if (initialized)
      return;

    ORecordSerializerFactory serializerFactory = ORecordSerializerFactory.instance();
    String serializeName = getStorage().getConfiguration().getRecordSerializer();
    if (serializeName == null) {
      throw new ODatabaseException(
          "Database created with orientdb version not supported anymore, use export+import to migrate the database");
    }
    serializer = serializerFactory.getFormat(serializeName);
    if (serializer == null)
      throw new ODatabaseException("RecordSerializer with name '" + serializeName + "' not found ");
    if (getStorage().getConfiguration().getRecordSerializerVersion() > serializer.getMinSupportedVersion())
      throw new ODatabaseException("Persistent record serializer version is not support by the current implementation");

    componentsFactory = getStorage().getComponentsFactory();

    localCache.startup();

    user = null;

    metadata = new OMetadataDefault(this);
    metadata.load();

    recordFormat = DEF_RECORD_FORMAT;

    if (!(getStorage() instanceof OStorageProxy)) {
      if (metadata.getIndexManager().autoRecreateIndexesAfterCrash()) {
        metadata.getIndexManager().recreateIndexes();

        activateOnCurrentThread();
        user = null;
      }

      installHooksEmbedded();
      registerHook(new OCommandCacheHook(this), ORecordHook.HOOK_POSITION.REGULAR);
      registerHook(new OSecurityTrackerHook(metadata.getSecurity(), this), ORecordHook.HOOK_POSITION.LAST);

      user = null;
    } else if (iUserName != null && iUserPassword != null) {
      user = new OImmutableUser(-1, new OUser(iUserName, OUser.encryptPassword(iUserPassword))
          .addRole(new ORole("passthrough", null, ORole.ALLOW_MODES.ALLOW_ALL_BUT)));
      installHooksRemote();
    }

    initialized = true;
  }

  private void installHooksEmbedded() {
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

  private void installHooksRemote() {
    hooks.clear();
    registerHook(new ClassIndexManagerRemote(this), ORecordHook.HOOK_POSITION.LAST);
  }

  private void closeOnDelete() {
    if (status != STATUS.OPEN)
      return;

    if (currentIntent != null) {
      currentIntent.end(this);
      currentIntent = null;
    }

    resetListeners();

    if (storage != null)
      storage.close(true, true);

    storage = null;
    status = STATUS.CLOSED;
  }

  private void clearCustomInternal() {
    storage.getConfiguration().clearProperties();
  }

  private void removeCustomInternal(final String iName) {
    setCustomInternal(iName, null);
  }

  private void setCustomInternal(final String iName, final String iValue) {
    if (iValue == null || "null".equalsIgnoreCase(iValue))
      // REMOVE
      storage.getConfiguration().removeProperty(iName);
    else
      // SET
      storage.getConfiguration().setProperty(iName, iValue);

    storage.getConfiguration().update();
  }

  private void callbackHookFailure(ORecord record, boolean wasNew, byte[] stream) {
    if (stream != null && stream.length > 0)
      callbackHooks(wasNew ? ORecordHook.TYPE.CREATE_FAILED : ORecordHook.TYPE.UPDATE_FAILED, record);
  }

  private void callbackHookSuccess(final ORecord record, final boolean wasNew, final byte[] stream,
      final OStorageOperationResult<Integer> operationResult) {
    if (stream != null && stream.length > 0) {
      final ORecordHook.TYPE hookType;
      if (!operationResult.isMoved()) {
        hookType = wasNew ? ORecordHook.TYPE.AFTER_CREATE : ORecordHook.TYPE.AFTER_UPDATE;
      } else {
        hookType = wasNew ? ORecordHook.TYPE.CREATE_REPLICATED : ORecordHook.TYPE.UPDATE_REPLICATED;
      }
      callbackHooks(hookType, record);

    }
  }

  private void callbackHookFinalize(final ORecord record, final boolean wasNew, final byte[] stream) {
    if (stream != null && stream.length > 0) {
      final ORecordHook.TYPE hookType;
      hookType = wasNew ? ORecordHook.TYPE.FINALIZE_CREATION : ORecordHook.TYPE.FINALIZE_UPDATE;
      callbackHooks(hookType, record);

      clearDocumentTracking(record);
    }
  }

  private void clearDocumentTracking(final ORecord record) {
    if (record instanceof ODocument && ((ODocument) record).isTrackingChanges()) {
      ODocumentInternal.clearTrackData((ODocument) record);
    }
  }

  private void checkRecordClass(final OClass recordClass, final String iClusterName, final ORecordId rid) {
    if (getStorageVersions().classesAreDetectedByClusterId()) {
      final OClass clusterIdClass = metadata.getImmutableSchemaSnapshot().getClassByClusterId(rid.getClusterId());
      if (recordClass == null && clusterIdClass != null || clusterIdClass == null && recordClass != null || (recordClass != null
          && !recordClass.equals(clusterIdClass)))
        throw new IllegalArgumentException(
            "Record saved into cluster '" + iClusterName + "' should be saved with class '" + clusterIdClass
                + "' but has been created with class '" + recordClass + "'");
    }
  }

  private byte[] updateStream(final ORecord record) {
    ORecordSerializationContext.pullContext();

    ODirtyManager manager = ORecordInternal.getDirtyManager(record);
    Set<ORecord> newRecords = manager.getNewRecords();
    Set<ORecord> updatedRecords = manager.getUpdateRecords();
    manager.clearForSave();
    if (newRecords != null) {
      for (ORecord newRecord : newRecords) {
        if (newRecord != record)
          getTransaction().saveRecord(newRecord, null, OPERATION_MODE.SYNCHRONOUS, false, null, null);
      }
    }
    if (updatedRecords != null) {
      for (ORecord updatedRecord : updatedRecords) {
        if (updatedRecord != record)
          getTransaction().saveRecord(updatedRecord, null, OPERATION_MODE.SYNCHRONOUS, false, null, null);
      }
    }

    ORecordSerializationContext.pushContext();
    ORecordInternal.unsetDirty(record);
    record.setDirty();
    return record.toStream();
  }

  private void init() {
    currentTx = new OTransactionNoTx(this);
  }

  private OFreezableStorageComponent getFreezableStorage() {
    OStorage s = getStorage();
    if (s instanceof OFreezableStorageComponent)
      return (OFreezableStorageComponent) s;
    else {
      OLogManager.instance().error(this, "Storage of type " + s.getType() + " does not support freeze operation");
      return null;
    }
  }

  /**
   * @Internal
   */
  public interface RecordReader {
    ORawBuffer readRecord(OStorage storage, ORecordId rid, String fetchPlan, boolean ignoreCache, final int recordVersion)
        throws ORecordNotFoundException;
  }

  /**
   * @Internal
   */
  public static final class SimpleRecordReader implements RecordReader {
    private final boolean prefetchRecords;

    public SimpleRecordReader(boolean prefetchRecords) {
      this.prefetchRecords = prefetchRecords;
    }

    @Override
    public ORawBuffer readRecord(OStorage storage, ORecordId rid, String fetchPlan, boolean ignoreCache, final int recordVersion)
        throws ORecordNotFoundException {
      return storage.readRecord(rid, fetchPlan, ignoreCache, prefetchRecords, null).getResult();
    }
  }

  /**
   * @Internal
   */
  public static final class LatestVersionRecordReader implements RecordReader {
    @Override
    public ORawBuffer readRecord(OStorage storage, ORecordId rid, String fetchPlan, boolean ignoreCache, final int recordVersion)
        throws ORecordNotFoundException {
      return storage.readRecordIfVersionIsNotLatest(rid, fetchPlan, ignoreCache, recordVersion).getResult();
    }
  }

  public void checkIfActive() {
    final ODatabaseRecordThreadLocal tl = ODatabaseRecordThreadLocal.INSTANCE;
    final ODatabaseDocumentInternal currentDatabase = tl != null ? tl.getIfDefined() : null;
    if (currentDatabase != this)
      throw new IllegalStateException(
          "The current database instance (" + toString() + ") is not active on the current thread (" + Thread.currentThread()
              + "). Current active database is: " + currentDatabase);
  }

  @Override
  public int addBlobCluster(final String iClusterName, final Object... iParameters) {
    int id;
    if (getStorage() instanceof OStorageProxy) {
      id = command(new OCommandSQL("create blob cluster :1")).execute(iClusterName);
      getMetadata().getSchema().reload();
    } else {
      if (!existsCluster(iClusterName)) {
        id = addCluster(iClusterName, iParameters);
      } else
        id = getClusterIdByName(iClusterName);
      getMetadata().getSchema().addBlobCluster(id);
    }
    return id;
  }

  public Set<Integer> getBlobClusterIds() {
    return getMetadata().getSchema().getBlobClusters();
  }

  private void compileHooks() {
    final List<ORecordHook>[] intermediateHooksByScope = new List[ORecordHook.SCOPE.values().length];
    for (ORecordHook.SCOPE scope : ORecordHook.SCOPE.values())
      intermediateHooksByScope[scope.ordinal()] = new ArrayList<ORecordHook>();

    for (ORecordHook hook : hooks.keySet())
      for (ORecordHook.SCOPE scope : hook instanceof ORecordHook.Scoped ?
          ((ORecordHook.Scoped) hook).getScopes() :
          ORecordHook.SCOPE.values())
        intermediateHooksByScope[scope.ordinal()].add(hook);

    for (ORecordHook.SCOPE scope : ORecordHook.SCOPE.values()) {
      final int ordinal = scope.ordinal();
      final List<ORecordHook> scopeHooks = intermediateHooksByScope[ordinal];
      hooksByScope[ordinal] = scopeHooks.toArray(new ORecordHook[scopeHooks.size()]);
    }
  }

  public static Object executeWithRetries(final OCallable<Object, Integer> callback, final int maxRetry) {
    return executeWithRetries(callback, maxRetry, 0, null);
  }

  public static Object executeWithRetries(final OCallable<Object, Integer> callback, final int maxRetry,
      final int waitBetweenRetry) {
    return executeWithRetries(callback, maxRetry, waitBetweenRetry, null);
  }

  public static Object executeWithRetries(final OCallable<Object, Integer> callback, final int maxRetry, final int waitBetweenRetry,
      final ORecord[] recordToReloadOnRetry) {
    ONeedRetryException lastException = null;
    for (int retry = 0; retry < maxRetry; ++retry) {
      try {
        return callback.call(retry);
      } catch (ONeedRetryException e) {
        // SAVE LAST EXCEPTION AND RETRY
        lastException = e;

        if (recordToReloadOnRetry != null) {
          // RELOAD THE RECORDS
          for (ORecord r : recordToReloadOnRetry)
            r.reload();
        }

        if (waitBetweenRetry > 0)
          try {
            Thread.sleep(waitBetweenRetry);
          } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
            break;
          }
      }
    }
    throw lastException;
  }

}
