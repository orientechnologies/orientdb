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
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.remote.OLiveQueryClientListener;
import com.orientechnologies.orient.client.remote.ORemoteQueryResult;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.client.remote.message.OLockRecordResponse;
import com.orientechnologies.orient.client.remote.message.ORemoteResultSet;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.cache.OLocalRecordCache;
import com.orientechnologies.orient.core.command.script.OCommandScriptException;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.fetch.OFetchHelper;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OClassIndexManager;
import com.orientechnologies.orient.core.index.OIndexManagerRemote;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.OSchemaRemote;
import com.orientechnologies.orient.core.metadata.security.*;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.OBasicTransaction;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorage.LOCKING_STRATEGY;
import com.orientechnologies.orient.core.storage.impl.local.OMicroTransaction;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OOfflineClusterException;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordSerializationContext;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import static com.orientechnologies.orient.core.storage.OStorage.LOCKING_STRATEGY.*;

/**
 * Created by tglman on 30/06/16.
 */
public class ODatabaseDocumentRemote extends ODatabaseDocumentAbstract {

  protected OStorageRemoteSession sessionMetadata;
  private   OrientDBConfig        config;
  private   OStorageRemote        storage;

  public ODatabaseDocumentRemote(final OStorageRemote storage) {
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

  @Deprecated
  public <DB extends ODatabase> DB open(final OToken iToken) {
    throw new UnsupportedOperationException("Deprecated Method");
  }

  @Override
  public <DB extends ODatabase> DB create() {
    throw new UnsupportedOperationException("Deprecated Method");
  }

  @Override
  public <DB extends ODatabase> DB create(String incrementalBackupPath) {
    throw new UnsupportedOperationException("use OrientDB");
  }

  @Override
  public <DB extends ODatabase> DB create(final Map<OGlobalConfiguration, Object> iInitialSettings) {
    throw new UnsupportedOperationException("use OrientDB");
  }

  @Override
  public void drop() {
    throw new UnsupportedOperationException("use OrientDB");
  }

  @Override
  public <DB extends ODatabase> DB set(ATTRIBUTES iAttribute, Object iValue) {

    if (iAttribute == ATTRIBUTES.CUSTOM) {
      String stringValue = iValue.toString();
      int indx = stringValue != null ? stringValue.indexOf('=') : -1;
      if (indx < 0) {
        if ("clear".equalsIgnoreCase(stringValue)) {
          String query = "alter database CUSTOM 'clear'";
          //Bypass the database command for avoid transaction management
          ORemoteQueryResult result = getStorage().command(this, query, new Object[] { iValue });
          result.getResult().close();
        } else
          throw new IllegalArgumentException("Syntax error: expected <name> = <value> or clear, instead found: " + iValue);
      } else {
        String customName = stringValue.substring(0, indx).trim();
        String customValue = stringValue.substring(indx + 1).trim();
        setCustom(customName, customValue);
      }
    } else {
      String query = "alter database " + iAttribute.name() + " ? ";
      //Bypass the database command for avoid transaction management
      ORemoteQueryResult result = getStorage().command(this, query, new Object[] { iValue });
      result.getResult().close();
      getStorage().reload();
    }

    return (DB) this;
  }

  @Override
  public <DB extends ODatabase> DB setCustom(String name, Object iValue) {
    if ("clear".equals(name) && iValue == null) {
      String query = "alter database CUSTOM 'clear'";
      //Bypass the database command for avoid transaction management
      ORemoteQueryResult result = getStorage().command(this, query, new Object[] {});
      result.getResult().close();
    } else {
      String query = "alter database CUSTOM  " + name + " = ?";
      //Bypass the database command for avoid transaction management
      ORemoteQueryResult result = getStorage().command(this, query, new Object[] { iValue });
      result.getResult().close();
      getStorage().reload();
    }
    return (DB) this;
  }

  public ODatabaseDocumentInternal copy() {
    ODatabaseDocumentRemote database = new ODatabaseDocumentRemote(storage);
    database.storage = storage.copy(this, database);
    database.storage.addUser();
    database.status = STATUS.OPEN;
    database.applyAttributes(config);
    database.initAtFirstOpen(this.getSharedContext());
    database.user = this.user;
    this.activateOnCurrentThread();
    return database;
  }

  @Override
  public boolean exists() {
    throw new UnsupportedOperationException("use OrientDB");
  }

  public void internalOpen(String user, String password, OrientDBConfig config, OSharedContext ctx) {
    this.config = config;
    applyAttributes(config);
    applyListeners(config);
    try {

      storage.open(user, password, config.getConfigurations());

      status = STATUS.OPEN;

      initAtFirstOpen(ctx);
      this.user = new OImmutableUser(-1,
          new OUser(user, password).addRole(new ORole("passthrough", null, ORole.ALLOW_MODES.ALLOW_ALL_BUT)));

      // WAKE UP LISTENERS
      callOnOpenListeners();

    } catch (OException e) {
      close();
      throw e;
    } catch (Exception e) {
      close();
      throw OException.wrapException(new ODatabaseException("Cannot open database url=" + getURL()), e);
    }
  }

  private void applyAttributes(OrientDBConfig config) {
    for (Entry<ATTRIBUTES, Object> attrs : config.getAttributes().entrySet()) {
      this.set(attrs.getKey(), attrs.getValue());
    }
  }

  private void initAtFirstOpen(OSharedContext ctx) {
    if (initialized)
      return;

    ORecordSerializerFactory serializerFactory = ORecordSerializerFactory.instance();
    serializer = serializerFactory.getFormat(ORecordSerializerNetworkV37.NAME);
    localCache.startup();
    componentsFactory = getStorage().getComponentsFactory();
    user = null;

    loadMetadata(ctx);

    initialized = true;
  }

  @Override
  protected void loadMetadata() {
    loadMetadata(this.getSharedContext());
  }

  protected void loadMetadata(OSharedContext ctx) {
    metadata = new OMetadataDefault(this);
    sharedContext = ctx;
    metadata.init(sharedContext);
    sharedContext.load(this);
  }

  private void applyListeners(OrientDBConfig config) {
    for (ODatabaseListener listener : config.getListeners()) {
      registerListener(listener);
    }
  }

  public ODatabaseDocumentAbstract begin(final OTransaction.TXTYPE iType) {
    checkOpenness();
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
      } catch (Exception t) {
        OLogManager.instance().error(this, "Error before tx begin", t);
      } catch (Error e) {
        OLogManager.instance().error(this, "Error before tx begin", e);
        throw e;
      }

    switch (iType) {
    case NOTX:
      setDefaultTransactionMode(null);
      break;

    case OPTIMISTIC:
      currentTx = new OTransactionOptimisticClient(this);
      break;

    case PESSIMISTIC:
      throw new UnsupportedOperationException("Pessimistic transaction");
    }

    currentTx.begin();
    return this;
  }

  public OStorageRemoteSession getSessionMetadata() {
    return sessionMetadata;
  }

  public void setSessionMetadata(OStorageRemoteSession sessionMetadata) {
    this.sessionMetadata = sessionMetadata;
  }

  @Override
  public OStorageRemote getStorage() {
    return storage;
  }

  @Override
  public void replaceStorage(OStorage iNewStorage) {
    throw new UnsupportedOperationException("unsupported replace of storage for remote database");
  }

  private void checkAndSendTransaction() {
    if (this.currentTx.isActive() && ((OTransactionOptimistic) this.currentTx).isChanged()) {
      if (((OTransactionOptimistic) this.getTransaction()).isAlreadyCleared())
        storage.reBeginTransaction(this, (OTransactionOptimistic) this.currentTx);
      else
        storage.beginTransaction(this, (OTransactionOptimistic) this.currentTx);
      ((OTransactionOptimistic) this.currentTx).resetChangesTracking();
    }
  }

  private void fetchTransacion() {
    storage.fetchTransaction(this);
  }

  @Override
  public OResultSet query(String query, Object[] args) {
    checkOpenness();
    checkAndSendTransaction();
    ORemoteQueryResult result = storage.query(this, query, args);
    if (result.isTransactionUpdated())
      fetchTransacion();
    if (result.isReloadMetadata())
      reload();
    return result.getResult();
  }

  @Override
  public OResultSet query(String query, Map args) {
    checkOpenness();
    checkAndSendTransaction();
    ORemoteQueryResult result = storage.query(this, query, args);
    if (result.isTransactionUpdated())
      fetchTransacion();
    if (result.isReloadMetadata())
      reload();
    return result.getResult();
  }

  @Override
  public OResultSet indexQuery(String indexName, String query, Object... args) {
    checkOpenness();

    if (getTransaction().isActive()) {
      OTransactionIndexChanges changes = getTransaction().getIndexChanges(indexName);
      Set<String> changedIndexes = ((OTransactionOptimisticClient) getTransaction()).getIndexChanged();
      if (changedIndexes.contains(indexName) || changes != null) {
        checkAndSendTransaction();
      }
    }
    ORemoteQueryResult result = storage.command(this, query, args);
    if (result.isReloadMetadata())
      reload();
    return result.getResult();
  }

  @Override
  public OResultSet command(String query, Object... args) {
    checkOpenness();
    checkAndSendTransaction();
    ORemoteQueryResult result = storage.command(this, query, args);
    if (result.isTransactionUpdated())
      fetchTransacion();
    if (result.isReloadMetadata())
      reload();
    return result.getResult();
  }

  @Override
  public OResultSet command(String query, Map args) {
    checkOpenness();
    checkAndSendTransaction();
    ORemoteQueryResult result = storage.command(this, query, args);
    if (result.isTransactionUpdated())
      fetchTransacion();
    if (result.isReloadMetadata())
      reload();
    return result.getResult();
  }

  @Override
  public OResultSet execute(String language, String script, Object... args)
      throws OCommandExecutionException, OCommandScriptException {
    checkOpenness();
    checkAndSendTransaction();
    ORemoteQueryResult result = storage.execute(this, language, script, args);
    if (result.isTransactionUpdated())
      fetchTransacion();
    if (result.isReloadMetadata())
      reload();
    return result.getResult();
  }

  @Override
  public OResultSet execute(String language, String script, Map<String, ?> args)
      throws OCommandExecutionException, OCommandScriptException {
    checkOpenness();
    checkAndSendTransaction();
    ORemoteQueryResult result = storage.execute(this, language, script, args);
    if (result.isTransactionUpdated())
      fetchTransacion();
    if (result.isReloadMetadata())
      reload();
    return result.getResult();
  }

  public void closeQuery(String queryId) {
    storage.closeQuery(this, queryId);
    queryClosed(queryId);
  }

  public void fetchNextPage(ORemoteResultSet rs) {
    checkOpenness();
    checkAndSendTransaction();
    storage.fetchNextPage(this, rs);
  }

  @Override
  public OLiveQueryMonitor live(String query, OLiveQueryResultListener listener, Object... args) {
    return storage.liveQuery(this, query, new OLiveQueryClientListener(this.copy(), listener), args);
  }

  @Override
  public OLiveQueryMonitor live(String query, OLiveQueryResultListener listener, Map<String, ?> args) {
    return storage.liveQuery(this, query, new OLiveQueryClientListener(this.copy(), listener), args);
  }

  @Override
  public void recycle(ORecord record) {
    throw new UnsupportedOperationException();
  }

  protected OMicroTransaction beginMicroTransaction() {
    return null;
  }

  public static void deInit(OStorageRemote storage) {
    OSharedContext sharedContext = storage.removeResource(OSharedContext.class.getName());
    //This storage may not have been completely opened yet
    if (sharedContext != null)
      sharedContext.close();
  }

  public static void updateSchema(OStorageRemote storage, ODocument schema) {
//    storage.get
    OSharedContext shared = storage.getResource(OSharedContext.class.getName(), () -> {
      throw new IllegalStateException("No shared context set on remote storage!");
    });
    ((OSchemaRemote) shared.getSchema()).update(schema);
  }

  public static void updateIndexManager(OStorageRemote storage, ODocument indexManager) {
    OSharedContext shared = storage.getResource(OSharedContext.class.getName(), () -> {
      throw new IllegalStateException("No shared context set on remote storage!");
    });
    ((OIndexManagerRemote) shared.getIndexManager()).update(indexManager);
  }

  public static void updateFunction(OStorageRemote storage) {
    OSharedContext shared = storage.getResource(OSharedContext.class.getName(), () -> {
      throw new IllegalStateException("No shared context set on remote storage!");
    });
    (shared.getFunctionLibrary()).update();
  }

  public static void updateSequences(OStorageRemote storage) {
    OSharedContext shared = storage.getResource(OSharedContext.class.getName(), () -> {
      throw new IllegalStateException("No shared context set on remote storage!");
    });
    (shared.getSequenceLibrary()).update();
  }

  @Override
  public int addBlobCluster(final String iClusterName, final Object... iParameters) {
    int id;
    try (OResultSet resultSet = command("create blob cluster :1", iClusterName)) {
      assert resultSet.hasNext();
      OResult result = resultSet.next();
      assert result.getProperty("value") != null;
      id = result.getProperty("value");
      return id;
    }
  }

  @Override
  public void executeDeleteRecord(OIdentifiable record, int iVersion, boolean iRequired, OPERATION_MODE iMode,
      boolean prohibitTombstones) {
    OTransactionOptimisticClient tx = new OTransactionOptimisticClient(this) {
      @Override
      protected void checkTransaction() {
      }
    };
    tx.begin();
    tx.deleteRecord((ORecord) record, iMode);
    tx.commit();

  }

  @Override
  public OIdentifiable beforeCreateOperations(OIdentifiable id, String iClusterName) {
    checkClusterSecurity(ORole.PERMISSION_CREATE, id, iClusterName);
    ORecordHook.RESULT res = callbackHooks(ORecordHook.TYPE.BEFORE_CREATE, id);
    if (res == ORecordHook.RESULT.RECORD_CHANGED) {
      if (id instanceof ODocument) {
        ((ODocument) id).validate();
      }
      return id;
    } else if (res == ORecordHook.RESULT.RECORD_REPLACED) {
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
    ORecordHook.RESULT res = callbackHooks(ORecordHook.TYPE.BEFORE_UPDATE, id);
    if (res == ORecordHook.RESULT.RECORD_CHANGED) {
      if (id instanceof ODocument) {
        ((ODocument) id).validate();
      }
      return id;
    } else if (res == ORecordHook.RESULT.RECORD_REPLACED) {
      ORecord replaced = OHookReplacedRecordThreadLocal.INSTANCE.get();
      if (replaced instanceof ODocument) {
        ((ODocument) replaced).validate();
      }
      return replaced;
    }
    return null;
  }

  @Override
  public void beforeDeleteOperations(OIdentifiable id, String iClusterName) {
    checkClusterSecurity(ORole.PERMISSION_DELETE, id, iClusterName);
    callbackHooks(ORecordHook.TYPE.BEFORE_DELETE, id);
  }

  public void afterUpdateOperations(final OIdentifiable id) {
    callbackHooks(ORecordHook.TYPE.AFTER_UPDATE, id);
    if (id instanceof ODocument) {
      ODocument doc = (ODocument) id;
      OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(this, doc);
      if (clazz != null && getTransaction().isActive()) {
        List<OClassIndexManager.IndexChange> indexChanges = new ArrayList<>();
        OClassIndexManager.processIndexOnDelete(this, doc, indexChanges);
        OTransactionOptimisticClient tx = (OTransactionOptimisticClient) getTransaction();
        for (OClassIndexManager.IndexChange indexChange : indexChanges) {
          tx.addIndexChanged(indexChange.index.getName());
        }
      }
    }
  }

  public void afterCreateOperations(final OIdentifiable id) {
    callbackHooks(ORecordHook.TYPE.AFTER_CREATE, id);
    if (id instanceof ODocument) {
      ODocument doc = (ODocument) id;
      OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(this, doc);
      if (clazz != null && getTransaction().isActive()) {
        List<OClassIndexManager.IndexChange> indexChanges = new ArrayList<>();
        OClassIndexManager.processIndexOnCreate(this, doc, indexChanges);
        OTransactionOptimisticClient tx = (OTransactionOptimisticClient) getTransaction();
        for (OClassIndexManager.IndexChange indexChange : indexChanges) {
          tx.addIndexChanged(indexChange.index.getName());
        }

      }
    }
  }

  public void afterDeleteOperations(final OIdentifiable id) {
    callbackHooks(ORecordHook.TYPE.AFTER_DELETE, id);
    if (id instanceof ODocument) {
      ODocument doc = (ODocument) id;
      OImmutableClass clazz = ODocumentInternal.getImmutableSchemaClass(this, doc);
      if (clazz != null && getTransaction().isActive()) {
        List<OClassIndexManager.IndexChange> indexChanges = new ArrayList<>();
        OClassIndexManager.processIndexOnDelete(this, doc, indexChanges);
        OTransactionOptimisticClient tx = (OTransactionOptimisticClient) getTransaction();
        for (OClassIndexManager.IndexChange indexChange : indexChanges) {
          tx.addIndexChanged(indexChange.index.getName());
        }
      }
    }
  }

  @Override
  public boolean beforeReadOperations(OIdentifiable identifiable) {
    return callbackHooks(ORecordHook.TYPE.BEFORE_READ, identifiable) == ORecordHook.RESULT.SKIP;
  }

  @Override
  public void afterReadOperations(OIdentifiable identifiable) {
    callbackHooks(ORecordHook.TYPE.AFTER_READ, identifiable);
  }

  @Override
  public ORecord saveAll(ORecord iRecord, String iClusterName, OPERATION_MODE iMode, boolean iForceCreate,
      ORecordCallback<? extends Number> iRecordCreatedCallback, ORecordCallback<Integer> iRecordUpdatedCallback) {
    OTransactionOptimisticClient tx = new OTransactionOptimisticClient(this) {
      @Override
      protected void checkTransaction() {
      }
    };
    tx.begin();
    tx.saveRecord(iRecord, iClusterName, iMode, iForceCreate, iRecordCreatedCallback, iRecordUpdatedCallback);
    tx.commit();

    return iRecord;
  }

  /**
   * This method is internal, it can be subject to signature change or be removed, do not use.
   *
   * @Internal
   */
  public <RET extends ORecord> RET executeReadRecord(final ORecordId rid, ORecord iRecord, final int recordVersion,
      final String fetchPlan, final boolean ignoreCache, final boolean iUpdateCache, final boolean loadTombstones,
      final LOCKING_STRATEGY lockingStrategy, RecordReader recordReader) {
    checkOpenness();
    checkIfActive();

    getMetadata().makeThreadLocalSchemaSnapshot();
    try {
      checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, getClusterNameById(rid.getClusterId()));

      // SEARCH IN LOCAL TX
      ORecord record = getTransaction().getRecord(rid);
      if (record == OBasicTransaction.DELETED_RECORD)
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
        if (beforeReadOperations(record))
          return null;

        if (record.getInternalStatus() == ORecordElement.STATUS.NOT_LOADED)
          record.reload();

        if (lockingStrategy == KEEP_SHARED_LOCK) {
          OLogManager.instance()
              .warn(this, "You use deprecated record locking strategy: %s it may lead to deadlocks " + lockingStrategy);
          record.lock(false);

        } else if (lockingStrategy == KEEP_EXCLUSIVE_LOCK) {
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
      getMetadata().clearThreadLocalSchemaSnapshot();
    }
  }

  public String getClusterName(final ORecord record) {
    // DON'T ASSIGN CLUSTER WITH REMOTE: SERVER KNOWS THE RIGHT CLUSTER BASED ON LOCALITY
    return null;
  }

  @Override
  public void internalLockRecord(OIdentifiable iRecord, LOCKING_STRATEGY lockingStrategy) {
    checkAndSendTransaction();
    OStorageRemote remote = getStorage();
    // -1 value means default timeout
    remote.lockRecord(iRecord, lockingStrategy, -1);
  }

  @Override
  public void internalUnlockRecord(OIdentifiable iRecord) {
    OStorageRemote remote = getStorage();
    remote.unlockRecord(iRecord.getIdentity());
  }

  @Override
  public <RET extends ORecord> RET lock(ORID recordId) throws OLockException {
    checkOpenness();
    checkIfActive();
    pessimisticLockChecks(recordId);
    checkAndSendTransaction();
    OStorageRemote remote = getStorage();
    // -1 value means default timeout
    OLockRecordResponse response = remote.lockRecord(recordId, EXCLUSIVE_LOCK, -1);
    ORecord record = fillRecordFromNetwork(recordId, response.getRecordType(), response.getVersion(), response.getRecord());
    return (RET) record;
  }

  @Override
  public <RET extends ORecord> RET lock(ORID recordId, long timeout, TimeUnit timeoutUnit) throws OLockException {
    checkOpenness();
    checkIfActive();
    pessimisticLockChecks(recordId);
    checkAndSendTransaction();
    OStorageRemote remote = getStorage();
    OLockRecordResponse response = remote.lockRecord(recordId, EXCLUSIVE_LOCK, timeoutUnit.toMillis(timeout));
    ORecord record = fillRecordFromNetwork(recordId, response.getRecordType(), response.getVersion(), response.getRecord());
    return (RET) record;
  }

  private ORecord fillRecordFromNetwork(ORID recordId, byte recordType, int version, byte[] buffer) {
    beforeReadOperations(recordId);
    ORecord toFillRecord = getLocalCache().findRecord(recordId);
    if (toFillRecord == null)
      toFillRecord = Orient.instance().getRecordFactoryManager().newInstance(recordType, recordId.getClusterId(), this);
    ORecordInternal.fill(toFillRecord, recordId, version, buffer, false);
    getLocalCache().updateRecord(toFillRecord);
    afterReadOperations(recordId);
    return toFillRecord;
  }

  @Override
  public void unlock(ORID recordId) throws OLockException {
    checkOpenness();
    checkIfActive();
    internalUnlockRecord(recordId);
  }
}
