package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.OUncompletedCommit;
import com.orientechnologies.orient.core.cache.OLocalRecordCache;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.intent.OIntent;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.executor.OTodoResultSet;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.ORecordMetadata;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.tx.OTransaction;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static jdk.nashorn.internal.runtime.regexp.joni.constants.EncloseType.MEMORY;

/**
 * Created by tglman on 20/07/16.
 */
public class ODatabaseDocumentTx implements ODatabaseDocumentInternal {

  private static ConcurrentMap<String, OrientDBFactory> embedded = new ConcurrentHashMap<>();
  private static ConcurrentMap<String, OrientDBFactory> remote   = new ConcurrentHashMap<>();

  private       ODatabaseDocumentInternal internal;
  private final String                    url;
  private       OrientDBFactory           factory;
  private final String                    type;
  private final String                    dbName;
  private final String                    baseUrl;

  public ODatabaseDocumentTx(String url) {
    this.url = url;
    int typeIndex = url.indexOf(':');
    String remoteUrl = url.substring(typeIndex + 1);
    type = url.substring(0, typeIndex);
    int index = remoteUrl.lastIndexOf('/');
    if (index > 0) {
      baseUrl = remoteUrl.substring(0, index);
      dbName = remoteUrl.substring(index + 1);
    } else {
      baseUrl = "./";
      dbName = remoteUrl;
    }
    }

  private ODatabaseDocumentTx(ODatabaseDocumentTx other) {
    url = other.url;
    type = other.type;
    baseUrl = other.baseUrl;
    dbName = other.dbName;
    internal = other.internal.copy();
  }

  public static ORecordSerializer getDefaultSerializer() {
    return ODatabaseDocumentTxOrig.getDefaultSerializer();
  }

  public static void setDefaultSerializer(ORecordSerializer defaultSerializer) {
    ODatabaseDocumentTxOrig.setDefaultSerializer(defaultSerializer);
  }

  @Override
  public OCurrentStorageComponentsFactory getStorageVersions() {
    return internal.getStorageVersions();
  }

  @Override
  public OSBTreeCollectionManager getSbTreeCollectionManager() {
    return internal.getSbTreeCollectionManager();
  }

  @Override
  public OBinarySerializerFactory getSerializerFactory() {
    return internal.getSerializerFactory();
  }

  @Override
  public ORecordSerializer getSerializer() {
    return internal.getSerializer();
  }

  @Override
  public int assignAndCheckCluster(ORecord record, String iClusterName) {
    return internal.assignAndCheckCluster(record, iClusterName);
  }

  @Override
  public <RET extends ORecord> RET loadIfVersionIsNotLatest(ORID rid, int recordVersion, String fetchPlan, boolean ignoreCache)
      throws ORecordNotFoundException {
    return (RET) internal.loadIfVersionIsNotLatest(rid, recordVersion, fetchPlan, ignoreCache);
  }

  @Override
  public void reloadUser() {
    internal.reloadUser();
  }

  @Override
  public ORecordHook.RESULT callbackHooks(ORecordHook.TYPE type, OIdentifiable id) {
    return internal.callbackHooks(type, id);
  }

  @Override
  public <RET extends ORecord> RET executeReadRecord(ORecordId rid, ORecord iRecord, int recordVersion, String fetchPlan,
      boolean ignoreCache, boolean iUpdateCache, boolean loadTombstones, OStorage.LOCKING_STRATEGY lockingStrategy,
      RecordReader recordReader) {
    return internal
        .executeReadRecord(rid, iRecord, recordVersion, fetchPlan, ignoreCache, iUpdateCache, loadTombstones, lockingStrategy,
            recordReader);
  }

  @Override
  public <RET extends ORecord> RET executeSaveRecord(ORecord record, String clusterName, int ver, OPERATION_MODE mode,
      boolean forceCreate, ORecordCallback<? extends Number> recordCreatedCallback,
      ORecordCallback<Integer> recordUpdatedCallback) {
    return internal.executeSaveRecord(record, clusterName, ver, mode, forceCreate, recordCreatedCallback, recordUpdatedCallback);
  }

  @Override
  public void executeDeleteRecord(OIdentifiable record, int iVersion, boolean iRequired, OPERATION_MODE iMode,
      boolean prohibitTombstones) {
    internal.executeDeleteRecord(record, iVersion, iRequired, iMode, prohibitTombstones);
  }

  @Override
  public <RET extends ORecord> RET executeSaveEmptyRecord(ORecord record, String clusterName) {
    return internal.executeSaveEmptyRecord(record, clusterName);
  }

  @Override
  public void setDefaultTransactionMode() {
    internal.setDefaultTransactionMode();
  }

  @Override
  public OMetadataInternal getMetadata() {
    return internal.getMetadata();
  }

  @Override
  public <DB extends ODatabase<?>> DB registerHook(ORecordHook iHookImpl) {
    internal.registerHook(iHookImpl);
    return (DB) this;
  }

  @Override
  public <DB extends ODatabase<?>> DB registerHook(ORecordHook iHookImpl, ORecordHook.HOOK_POSITION iPosition) {
    internal.registerHook(iHookImpl, iPosition);
    return (DB) this;
  }

  @Override
  public Map<ORecordHook, ORecordHook.HOOK_POSITION> getHooks() {
    return internal.getHooks();
  }

  @Override
  public <DB extends ODatabase<?>> DB unregisterHook(ORecordHook iHookImpl) {
    internal.unregisterHook(iHookImpl);
    return (DB) this;
  }

  @Override
  public boolean isMVCC() {
    return false;
  }

  @Override
  public Iterable<ODatabaseListener> getListeners() {
    return internal.getListeners();
  }

  @Override
  public <DB extends ODatabase<?>> DB setMVCC(boolean iValue) {
    return null;
  }

  @Override
  public String getType() {
    return internal.getType();
  }

  @Override
  public ORecordConflictStrategy getConflictStrategy() {
    return internal.getConflictStrategy();
  }

  @Override
  public <DB extends ODatabase<?>> DB setConflictStrategy(String iStrategyName) {
    internal.setConflictStrategy(iStrategyName);
    return (DB) this;
  }

  @Override
  public <DB extends ODatabase<?>> DB setConflictStrategy(ORecordConflictStrategy iResolver) {
    internal.setConflictStrategy(iResolver);
    return (DB) this;
  }

  @Override
  public String incrementalBackup(String path) {
    return internal.incrementalBackup(path);
  }

  @Override
  public ODatabaseDocumentTx copy() {
    return new ODatabaseDocumentTx(this);
  }

  @Override
  public Set<ORecord> executeReadRecords(Set<ORecordId> iRids, boolean ignoreCache) {
    return internal.executeReadRecords(iRids, ignoreCache);
  }

  @Override
  public void checkIfActive() {
    internal.checkIfActive();
  }

  @Override
  public void callOnOpenListeners() {
    internal.callOnOpenListeners();
  }

  @Override
  public void callOnCloseListeners() {
    internal.callOnOpenListeners();
  }

  @Override
  public OStorage getStorage() {
    if (internal == null)
      return null;
    return internal.getStorage();
  }

  @Override
  public void setUser(OSecurityUser user) {
    internal.setUser(user);
  }

  @Override
  public void replaceStorage(OStorage iNewStorage) {
    internal.replaceStorage(iNewStorage);
  }

  @Override
  public <V> V callInLock(Callable<V> iCallable, boolean iExclusiveLock) {
    return internal.callInLock(iCallable, iExclusiveLock);
  }

  @Override
  public void resetInitialization() {
    internal.resetInitialization();
  }

  @Override
  public ODatabaseInternal<?> getDatabaseOwner() {
    internal.getDatabaseOwner();
    return this;
  }

  @Override
  public ODatabaseInternal<?> setDatabaseOwner(ODatabaseInternal<?> iOwner) {
    internal.setDatabaseOwner(iOwner);
    return this;
  }

  @Override
  public <DB extends ODatabase> DB getUnderlying() {
    return internal.getUnderlying();
  }

  @Override
  public void setInternal(ATTRIBUTES attribute, Object iValue) {
    internal.setInternal(attribute, iValue);
  }

  @Override
  public <DB extends ODatabase> DB open(OToken iToken) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OUncompletedCommit<Void> initiateCommit() {
    return internal.initiateCommit();
  }

  @Override
  public OUncompletedCommit<Void> initiateCommit(boolean force) {
    return internal.initiateCommit(force);
  }

  @Override
  public OSharedContext getSharedContext() {
    return internal.getSharedContext();
  }

  @Override
  public ORecordIteratorClass<ODocument> browseClass(String iClassName) {
    return internal.browseClass(iClassName);
  }

  @Override
  public ORecordIteratorClass<ODocument> browseClass(String iClassName, boolean iPolymorphic) {
    return internal.browseClass(iClassName, iPolymorphic);
  }

  @Override
  public void freeze() {
    internal.freeze();
  }

  @Override
  public void release() {
    internal.release();
  }

  @Override
  public void freeze(boolean throwException) {
    internal.freeze(throwException);
  }

  @Override
  public ODocument newInstance() {
    return internal.newInstance();
  }

  @Override
  public ODictionary<ORecord> getDictionary() {
    return internal.getDictionary();
  }

  @Override
  public OSecurityUser getUser() {
    return internal.getUser();
  }

  @Override
  public <RET extends ORecord> RET load(ORecord iObject) {
    return internal.load(iObject);
  }

  @Override
  public <RET extends ORecord> RET load(ORecord iObject, String iFetchPlan) {
    return internal.load(iObject, iFetchPlan);
  }

  @Override
  public <RET extends ORecord> RET load(ORecord iObject, String iFetchPlan, boolean iIgnoreCache, boolean loadTombstone,
      OStorage.LOCKING_STRATEGY iLockingStrategy) {
    return internal.load(iObject, iFetchPlan, iIgnoreCache, loadTombstone, iLockingStrategy);
  }

  @Override
  public <RET extends ORecord> RET load(ORecord iObject, String iFetchPlan, boolean iIgnoreCache, boolean iUpdateCache,
      boolean loadTombstone, OStorage.LOCKING_STRATEGY iLockingStrategy) {
    return internal.load(iObject, iFetchPlan, iIgnoreCache, iUpdateCache, loadTombstone, iLockingStrategy);
  }

  @Override
  public <RET extends ORecord> RET load(ORecord iObject, String iFetchPlan, boolean iIgnoreCache) {
    return internal.load(iObject, iFetchPlan, iIgnoreCache);
  }

  @Override
  public <RET extends ORecord> RET reload(ORecord iObject, String iFetchPlan, boolean iIgnoreCache) {
    return internal.reload(iObject, iFetchPlan, iIgnoreCache);
  }

  @Override
  public <RET extends ORecord> RET reload(ORecord iObject, String iFetchPlan, boolean iIgnoreCache, boolean force) {
    return internal.reload(iObject, iFetchPlan, iIgnoreCache, force);
  }

  @Override
  public <RET extends ORecord> RET load(ORID recordId) {
    return internal.load(recordId);
  }

  @Override
  public <RET extends ORecord> RET load(ORID iRecordId, String iFetchPlan) {
    return internal.load(iRecordId, iFetchPlan);
  }

  @Override
  public <RET extends ORecord> RET load(ORID iRecordId, String iFetchPlan, boolean iIgnoreCache) {
    return internal.load(iRecordId, iFetchPlan, iIgnoreCache);
  }

  @Override
  public <RET extends ORecord> RET load(ORID iRecordId, String iFetchPlan, boolean iIgnoreCache, boolean loadTombstone,
      OStorage.LOCKING_STRATEGY iLockingStrategy) {
    return internal.load(iRecordId, iFetchPlan, iIgnoreCache, loadTombstone, iLockingStrategy);
  }

  @Override
  public <RET extends ORecord> RET load(ORID iRecordId, String iFetchPlan, boolean iIgnoreCache, boolean iUpdateCache,
      boolean loadTombstone, OStorage.LOCKING_STRATEGY iLockingStrategy) {
    return internal.load(iRecordId, iFetchPlan, iIgnoreCache, iUpdateCache, loadTombstone, iLockingStrategy);
  }

  @Override
  public <RET extends ORecord> RET save(ORecord iObject) {
    return internal.save(iObject);
  }

  @Override
  public <RET extends ORecord> RET save(ORecord iObject, OPERATION_MODE iMode, boolean iForceCreate,
      ORecordCallback<? extends Number> iRecordCreatedCallback, ORecordCallback<Integer> iRecordUpdatedCallback) {
    return internal.save(iObject, iMode, iForceCreate, iRecordCreatedCallback, iRecordUpdatedCallback);
  }

  @Override
  public <RET extends ORecord> RET save(ORecord iObject, String iClusterName) {
    return internal.save(iObject, iClusterName);
  }

  @Override
  public <RET extends ORecord> RET save(ORecord iObject, String iClusterName, OPERATION_MODE iMode, boolean iForceCreate,
      ORecordCallback<? extends Number> iRecordCreatedCallback, ORecordCallback<Integer> iRecordUpdatedCallback) {
    return internal.save(iObject, iClusterName, iMode, iForceCreate, iRecordCreatedCallback, iRecordUpdatedCallback);
  }

  @Override
  public ODatabase<ORecord> delete(ORecord iObject) {
    internal.delete(iObject);
    return this;
  }

  @Override
  public ODatabase<ORecord> delete(ORID iRID) {
    internal.delete(iRID);
    return this;
  }

  @Override
  public ODatabase<ORecord> delete(ORID iRID, int iVersion) {
    internal.delete(iRID, iVersion);
    return this;
  }

  @Override
  public boolean hide(ORID rid) {
    return internal.hide(rid);
  }

  @Override
  public ODatabase<ORecord> cleanOutRecord(ORID rid, int version) {
    internal.cleanOutRecord(rid, version);
    return this;
  }

  @Override
  public OTransaction getTransaction() {
    return internal.getTransaction();
  }

  @Override
  public ODatabase<ORecord> begin() {
    internal.begin();
    return this;
  }

  @Override
  public ODatabase<ORecord> begin(OTransaction.TXTYPE iStatus) {
    internal.begin(iStatus);
    return this;
  }

  @Override
  public ODatabase<ORecord> begin(OTransaction iTx) throws OTransactionException {
    internal.begin(iTx);
    return this;
  }

  @Override
  public ODatabase<ORecord> commit() throws OTransactionException {
    internal.commit();
    return this;
  }

  @Override
  public ODatabase<ORecord> commit(boolean force) throws OTransactionException {
    internal.commit(force);
    return this;
  }

  @Override
  public ODatabase<ORecord> rollback() throws OTransactionException {
    internal.rollback();
    return this;
  }

  @Override
  public ODatabase<ORecord> rollback(boolean force) throws OTransactionException {
    internal.rollback(force);
    return this;
  }

  @Override
  public <RET extends List<?>> RET query(OQuery<?> iCommand, Object... iArgs) {
    return internal.query(iCommand, iArgs);
  }

  @Override
  public <RET extends OCommandRequest> RET command(OCommandRequest iCommand) {
    return internal.command(iCommand);
  }

  @Override
  public ORecordIteratorCluster<ODocument> browseCluster(String iClusterName) {
    return internal.browseCluster(iClusterName);
  }

  @Override
  public ORecordIteratorCluster<ODocument> browseCluster(String iClusterName, long startClusterPosition, long endClusterPosition,
      boolean loadTombstones) {
    return internal.browseCluster(iClusterName, startClusterPosition, endClusterPosition, loadTombstones);
  }

  @Override
  public <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(String iClusterName, Class<REC> iRecordClass) {
    return internal.browseCluster(iClusterName, iRecordClass);
  }

  @Override
  public <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(String iClusterName, Class<REC> iRecordClass,
      long startClusterPosition, long endClusterPosition) {
    return internal.browseCluster(iClusterName, iRecordClass, startClusterPosition, endClusterPosition);
  }

  @Override
  public <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(String iClusterName, Class<REC> iRecordClass,
      long startClusterPosition, long endClusterPosition, boolean loadTombstones) {
    return internal.browseCluster(iClusterName, iRecordClass, startClusterPosition, endClusterPosition, loadTombstones);
  }

  @Override
  public <RET extends ORecord> RET getRecord(OIdentifiable iIdentifiable) {
    return internal.getRecord(iIdentifiable);
  }

  @Override
  public byte getRecordType() {
    return internal.getRecordType();
  }

  @Override
  public boolean isRetainRecords() {
    return internal.isRetainRecords();
  }

  @Override
  public ODatabaseDocument setRetainRecords(boolean iValue) {
    return internal.setRetainRecords(iValue);
  }

  @Override
  public <DB extends ODatabaseDocument> DB checkSecurity(ORule.ResourceGeneric resourceGeneric, String resourceSpecific,
      int iOperation) {
    internal.checkSecurity(resourceGeneric, resourceSpecific, iOperation);
    return (DB) this;
  }

  @Override
  public <DB extends ODatabaseDocument> DB checkSecurity(ORule.ResourceGeneric iResourceGeneric, int iOperation,
      Object iResourceSpecific) {
    internal.checkSecurity(iResourceGeneric, iOperation, iResourceSpecific);
    return (DB) this;
  }

  @Override
  public <DB extends ODatabaseDocument> DB checkSecurity(ORule.ResourceGeneric iResourceGeneric, int iOperation,
      Object... iResourcesSpecific) {
    internal.checkSecurity(iResourceGeneric, iOperation, iResourcesSpecific);
    return (DB) this;
  }

  @Override
  public boolean isValidationEnabled() {
    return internal.isValidationEnabled();
  }

  @Override
  public <DB extends ODatabaseDocument> DB setValidationEnabled(boolean iEnabled) {
    internal.setValidationEnabled(iEnabled);
    return (DB) this;
  }

  @Override
  public <DB extends ODatabaseDocument> DB checkSecurity(String iResource, int iOperation) {
    internal.checkSecurity(iResource, iOperation);
    return (DB) this;
  }

  @Override
  public <DB extends ODatabaseDocument> DB checkSecurity(String iResourceGeneric, int iOperation, Object iResourceSpecific) {
    internal.checkSecurity(iResourceGeneric, iOperation, iResourceSpecific);
    return (DB) this;
  }

  @Override
  public <DB extends ODatabaseDocument> DB checkSecurity(String iResourceGeneric, int iOperation, Object... iResourcesSpecific) {
    internal.checkSecurity(iResourceGeneric, iOperation, iResourcesSpecific);
    return (DB) this;
  }

  @Override
  public boolean isPooled() {
    return internal.isPooled();
  }

  @Override
  public <DB extends ODatabase> DB open(String iUserName, String iUserPassword) {
    if ("remote".equals(type)) {
      synchronized (remote) {
        factory = remote.get(baseUrl);
        if (factory == null) {
          factory = OrientDBFactory.fromUrl("remote:" + baseUrl, null);
          remote.put(baseUrl, factory);
        }
      }
      internal = (ODatabaseDocumentInternal) factory.open(dbName, iUserName, iUserPassword);
    } else {
      synchronized (embedded) {
        factory = embedded.get(baseUrl);
        if (factory == null) {
          factory = OrientDBFactory.fromUrl("embedded:" + baseUrl, null);
          embedded.put(baseUrl, factory);
        }
      }
      internal = (ODatabaseDocumentInternal) factory.open(dbName, iUserName, iUserPassword);
    }
    return (DB) this;
  }

  @Override
  public <DB extends ODatabase> DB create() {
    //TODO
    return create((Map<OGlobalConfiguration, Object>) null);
  }

  @Override
  public <DB extends ODatabase> DB create(String incrementalBackupPath) {
    return null;
  }

  @Override
  public <DB extends ODatabase> DB create(Map<OGlobalConfiguration, Object> iInitialSettings) {
    if ("remote".equals(type)) {
      throw new UnsupportedOperationException();
    } else if ("memory".equals(type)) {
      synchronized (embedded) {
        factory = embedded.get(baseUrl);
        if (factory == null) {
          factory = OrientDBFactory.fromUrl("embedded:" + baseUrl, null);
          embedded.put(baseUrl, factory);
        }
      }
      factory.create(dbName, null, null, OrientDBFactory.DatabaseType.MEMORY);
      internal = (ODatabaseDocumentInternal) factory.open(dbName, "admin", "admin");

    } else {
      synchronized (embedded) {
        factory = embedded.get(baseUrl);
        if (factory == null) {
          factory = OrientDBFactory.fromUrl("embedded:" + baseUrl, null);
          embedded.put(baseUrl, factory);
        }
      }
      factory.create(dbName, null, null, OrientDBFactory.DatabaseType.PLOCAL);
      internal = (ODatabaseDocumentInternal) factory.open(dbName, "admin", "admin");
    }
    return (DB) this;

  }

  @Override
  public ODatabase activateOnCurrentThread() {
    if (internal != null)
      internal.activateOnCurrentThread();
    return this;
  }

  @Override
  public boolean isActiveOnCurrentThread() {
    return internal.isActiveOnCurrentThread();
  }

  @Override
  public void reload() {
    internal.reload();
  }

  @Override
  public void drop() {
    //TODO
    factory.drop(this.getName(), null, null);
  }

  @Override
  public OContextConfiguration getConfiguration() {
    return internal.getConfiguration();
  }

  @Override
  public boolean declareIntent(OIntent iIntent) {
    return internal.declareIntent(iIntent);
  }

  @Override
  public boolean exists() {
    if ("remote".equals(type)) {
      throw new UnsupportedOperationException();
    } else {
      synchronized (embedded) {
        factory = embedded.get(baseUrl);
        if (factory == null) {
          factory = OrientDBFactory.fromUrl("embedded:" + baseUrl, null);
          embedded.put(baseUrl, factory);
        }
      }
      return factory.exists(dbName, null, null);
    }
  }

  @Override
  public void close() {
    //TODO
  }

  @Override
  public STATUS getStatus() {
    return internal.getStatus();
  }

  @Override
  public <DB extends ODatabase> DB setStatus(STATUS iStatus) {
    internal.setStatus(iStatus);
    return (DB) this;
  }

  @Override
  public long getSize() {
    return internal.getSize();
  }

  @Override
  public String getName() {
    return internal.getName();
  }

  @Override
  public String getURL() {
    return internal.getURL();
  }

  @Override
  public OLocalRecordCache getLocalCache() {
    return internal.getLocalCache();
  }

  @Override
  public int getDefaultClusterId() {
    return internal.getDefaultClusterId();
  }

  @Override
  public int getClusters() {
    return internal.getClusters();
  }

  @Override
  public boolean existsCluster(String iClusterName) {
    return internal.existsCluster(iClusterName);
  }

  @Override
  public Collection<String> getClusterNames() {
    return internal.getClusterNames();
  }

  @Override
  public int getClusterIdByName(String iClusterName) {
    return internal.getClusterIdByName(iClusterName);
  }

  @Override
  public String getClusterNameById(int iClusterId) {
    return internal.getClusterNameById(iClusterId);
  }

  @Override
  public long getClusterRecordSizeByName(String iClusterName) {
    return internal.getClusterRecordSizeByName(iClusterName);
  }

  @Override
  public long getClusterRecordSizeById(int iClusterId) {
    return internal.getClusterRecordSizeById(iClusterId);
  }

  @Override
  public boolean isClosed() {
    return internal == null || internal.isClosed();
  }

  @Override
  public void truncateCluster(String clusterName) {
    internal.truncateCluster(clusterName);
  }

  @Override
  public long countClusterElements(int iCurrentClusterId) {
    return internal.countClusterElements(iCurrentClusterId);
  }

  @Override
  public long countClusterElements(int iCurrentClusterId, boolean countTombstones) {
    return internal.countClusterElements(iCurrentClusterId, countTombstones);
  }

  @Override
  public long countClusterElements(int[] iClusterIds) {
    return internal.countClusterElements(iClusterIds);
  }

  @Override
  public long countClusterElements(int[] iClusterIds, boolean countTombstones) {
    return internal.countClusterElements(iClusterIds, countTombstones);
  }

  @Override
  public long countClusterElements(String iClusterName) {
    return internal.countClusterElements(iClusterName);
  }

  @Override
  public int addCluster(String iClusterName, Object... iParameters) {
    return internal.addCluster(iClusterName, iParameters);
  }

  @Override
  public int addBlobCluster(String iClusterName, Object... iParameters) {
    return internal.addBlobCluster(iClusterName, iParameters);
  }

  @Override
  public Set<Integer> getBlobClusterIds() {
    return internal.getBlobClusterIds();
  }

  @Override
  public int addCluster(String iClusterName, int iRequestedId, Object... iParameters) {
    return internal.addCluster(iClusterName, iRequestedId, iParameters);
  }

  @Override
  public boolean dropCluster(String iClusterName, boolean iTruncate) {
    return internal.dropCluster(iClusterName, iTruncate);
  }

  @Override
  public boolean dropCluster(int iClusterId, boolean iTruncate) {
    return internal.dropCluster(iClusterId, iTruncate);
  }

  @Override
  public Object setProperty(String iName, Object iValue) {
    return internal.setProperty(iName, iValue);
  }

  @Override
  public Object getProperty(String iName) {
    return internal.getProperty(iName);
  }

  @Override
  public Iterator<Map.Entry<String, Object>> getProperties() {
    return internal.getProperties();
  }

  @Override
  public Object get(ATTRIBUTES iAttribute) {
    return internal.get(iAttribute);
  }

  @Override
  public <DB extends ODatabase> DB set(ATTRIBUTES iAttribute, Object iValue) {
    internal.set(iAttribute, iValue);
    return (DB) this;
  }

  @Override
  public void registerListener(ODatabaseListener iListener) {
    internal.registerListener(iListener);
  }

  @Override
  public void unregisterListener(ODatabaseListener iListener) {
    internal.unregisterListener(iListener);
  }

  @Override
  public ORecordMetadata getRecordMetadata(ORID rid) {
    return internal.getRecordMetadata(rid);
  }

  @Override
  public ODocument newInstance(String iClassName) {
    return internal.newInstance(iClassName);
  }

  @Override
  public long countClass(String iClassName) {
    return internal.countClass(iClassName);
  }

  @Override
  public long countClass(String iClassName, boolean iPolymorphic) {
    return internal.countClass(iClassName, iPolymorphic);
  }

  @Override
  public List<String> backup(OutputStream out, Map<String, Object> options, Callable<Object> callable,
      OCommandOutputListener iListener, int compressionLevel, int bufferSize) throws IOException {
    return internal.backup(out, options, callable, iListener, compressionLevel, bufferSize);
  }

  @Override
  public void restore(InputStream in, Map<String, Object> options, Callable<Object> callable, OCommandOutputListener iListener)
      throws IOException {
    internal.restore(in, options, callable, iListener);
  }

  public void setSerializer(ORecordSerializer serializer) {
    ((ODatabaseDocumentTxOrig) internal).setSerializer(serializer);
  }
  
  @Override
  public OTodoResultSet query(String query, Object... args) {
    return internal.query(query, args);
  }

  @Override
  public OTodoResultSet query(String query, Map args) throws OCommandSQLParsingException, OCommandExecutionException {
    return internal.query(query, args);
  }  
  
  @Override
  public OTodoResultSet command(String query, Map args) throws OCommandSQLParsingException, OCommandExecutionException {
    return internal.command(query, args);
  }
  
  @Override
  public OTodoResultSet command(String query, Object... args) throws OCommandSQLParsingException, OCommandExecutionException {
    return internal.command(query, args);
  }

  @Override
  public <DB extends ODatabase> DB setCustom(String name, Object iValue) {
    return internal.setCustom(name, iValue);
  }
  
  @Override
  public boolean isPrefetchRecords() {
    return internal.isPrefetchRecords();
  }
  
  public void setPrefetchRecords(boolean prefetchRecords) {
    internal.setPrefetchRecords(prefetchRecords);
  };
  
}
