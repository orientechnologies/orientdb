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
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.client.remote.OLiveQueryClientListener;
import com.orientechnologies.orient.client.remote.ORemoteQueryResult;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.client.remote.message.ORemoteResultSet;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.cache.OLocalRecordCache;
import com.orientechnologies.orient.core.command.script.OCommandScriptException;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBagDeleter;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OClassIndexManager;
import com.orientechnologies.orient.core.index.OIndexManagerRemote;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.schema.OSchemaRemote;
import com.orientechnologies.orient.core.metadata.security.*;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODirtyManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSaveThreadLocal;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerNetworkV37;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.*;
import com.orientechnologies.orient.core.storage.impl.local.OMicroTransaction;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordSerializationContext;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;

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
    database.initAtFirstOpen();
    database.user = this.user;
    this.activateOnCurrentThread();
    return database;
  }

  @Override
  public boolean exists() {
    throw new UnsupportedOperationException("use OrientDB");
  }

  public void internalOpen(String user, String password, OrientDBConfig config) {
    this.config = config;
    applyAttributes(config);
    applyListeners(config);
    try {

      storage.open(user, password, config.getConfigurations());

      status = STATUS.OPEN;

      initAtFirstOpen();
      this.user = new OImmutableUser(-1, new OUser(user, OUser.encryptPassword(password))
          .addRole(new ORole("passthrough", null, ORole.ALLOW_MODES.ALLOW_ALL_BUT)));

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

  private void initAtFirstOpen() {
    if (initialized)
      return;

    ORecordSerializerFactory serializerFactory = ORecordSerializerFactory.instance();
    serializer = serializerFactory.getFormat(ORecordSerializerNetworkV37.NAME);
    localCache.startup();
    componentsFactory = getStorage().getComponentsFactory();
    user = null;

    loadMetadata();

    initialized = true;
  }

  protected void loadMetadata() {
    metadata = new OMetadataDefault(this);
    sharedContext = getStorage().getResource(OSharedContext.class.getName(), new Callable<OSharedContext>() {
      @Override
      public OSharedContext call() throws Exception {
        OSharedContext shared = new OSharedContextRemote(getStorage());
        return shared;
      }
    });
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
      setDefaultTransactionMode();
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
  }

  @Override
  public void queryStarted(String id, OResultSet rs) {
    //do nothing
  }

  public void fetchNextPage(ORemoteResultSet rs) {
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
    OSharedContext shared = storage.getResource(OSharedContext.class.getName(), new Callable<OSharedContext>() {
      @Override
      public OSharedContext call() throws Exception {
        OSharedContext shared = new OSharedContextRemote(storage);
        return shared;
      }
    });
    ((OSchemaRemote) shared.getSchema()).update(schema);
  }

  public static void updateIndexManager(OStorageRemote storage, ODocument indexManager) {
    OSharedContext shared = storage.getResource(OSharedContext.class.getName(), new Callable<OSharedContext>() {
      @Override
      public OSharedContext call() throws Exception {
        OSharedContext shared = new OSharedContextRemote(storage);
        return shared;
      }
    });
    ((OIndexManagerRemote) shared.getIndexManager()).update(indexManager);
  }

  public static void updateFunction(OStorageRemote storage) {
    OSharedContext shared = storage.getResource(OSharedContext.class.getName(), new Callable<OSharedContext>() {
      @Override
      public OSharedContext call() throws Exception {
        OSharedContext shared = new OSharedContextRemote(storage);
        return shared;
      }
    });
    (shared.getFunctionLibrary()).update();

  }

  public static void updateSequences(OStorageRemote storage) {
    OSharedContext shared = storage.getResource(OSharedContext.class.getName(), new Callable<OSharedContext>() {
      @Override
      public OSharedContext call() throws Exception {
        OSharedContext shared = new OSharedContextRemote(storage);
        return shared;
      }
    });
    (shared.getSequenceLibrary()).update();
  }

  @Override
  public int addBlobCluster(final String iClusterName, final Object... iParameters) {
    int id;
    OResultSet resultSet = command("create blob cluster :1", iClusterName);
    assert resultSet.hasNext();
    OResult result = resultSet.next();
    assert result.getProperty("value") != null;
    id = result.getProperty("value");
    return id;
  }

  @Override
  public <RET extends ORecord> RET executeSaveRecord(ORecord record, String clusterName, int ver, OPERATION_MODE mode,
      boolean forceCreate, ORecordCallback<? extends Number> recordCreatedCallback,
      ORecordCallback<Integer> recordUpdatedCallback) {

    checkOpenness();
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

        ORecord overwritten;
        if (isNew) {
          // NOTIFY IDENTITY HAS CHANGED
          ORecordInternal.onBeforeIdentityChanged(record);
          int id = assignAndCheckCluster(record, clusterName);
          clusterName = getClusterNameById(id);
          overwritten = (ORecord) beforeCreateOperations(record, clusterName);
        } else {
          overwritten = (ORecord) beforeUpdateOperations(record, clusterName);
        }
        if (overwritten != null) {
          record = overwritten;
        }
        stream = getSerializer().toStream(record, false);

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
            final OStorageOperationResult<OPhysicalPosition> ppos = getStorage()
                .createRecord(rid, content, ver, recordType, modeIndex, (ORecordCallback<Long>) recordCreatedCallback);
            operationResult = new OStorageOperationResult<Integer>(ppos.getResult().recordVersion, ppos.isMoved());

          } else {
            // UPDATE
            operationResult = getStorage()
                .updateRecord(rid, updateContent, content, ver, recordType, modeIndex, recordUpdatedCallback);
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
          else if (version > record.getVersion() + 1 && getStorage() instanceof OStorageProxy)
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

  @Override
  public void executeDeleteRecord(OIdentifiable record, int iVersion, boolean iRequired, OPERATION_MODE iMode,
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
            final boolean result = getStorage().cleanOutRecord(rid, iVersion, iMode.ordinal(), null);
            if (!result && iRequired)
              throw new ORecordNotFoundException(rid);
            operationResult = new OStorageOperationResult<Boolean>(result);
          } else {
            final OStorageOperationResult<Boolean> result = getStorage().deleteRecord(rid, iVersion, iMode.ordinal(), null);
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

  protected byte[] updateStream(final ORecord record) {
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
    return serializer.toStream(record, false);
  }

  @Override
  public OIdentifiable beforeCreateOperations(OIdentifiable id, String iClusterName) {
    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_CREATE, iClusterName);
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
    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_UPDATE, iClusterName);
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
    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_DELETE, iClusterName);
    callbackHooks(ORecordHook.TYPE.BEFORE_DELETE, id);
  }

  public void afterUpdateOperations(final OIdentifiable id) {
    callbackHooks(ORecordHook.TYPE.AFTER_UPDATE, id);
  }

  public void afterCreateOperations(final OIdentifiable id) {
    callbackHooks(ORecordHook.TYPE.AFTER_CREATE, id);
  }

  public void afterDeleteOperations(final OIdentifiable id) {
    callbackHooks(ORecordHook.TYPE.AFTER_DELETE, id);
  }

}
