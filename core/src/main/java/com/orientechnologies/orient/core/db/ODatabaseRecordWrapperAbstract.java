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
package com.orientechnologies.orient.core.db;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.hook.ORecordHook.RESULT;
import com.orientechnologies.orient.core.hook.ORecordHook.TYPE;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.core.version.ORecordVersion;

@SuppressWarnings("unchecked")
public abstract class ODatabaseRecordWrapperAbstract<DB extends ODatabaseDocumentInternal> extends
    ODatabaseWrapperAbstract<DB, ORecord> implements ODatabaseDocumentInternal {

  public ODatabaseRecordWrapperAbstract(final DB iDatabase) {
    super(iDatabase);
    iDatabase.setDatabaseOwner(this);
  }

  @Override
  public <THISDB extends ODatabase> THISDB create() {
    checkSecurity(ORule.ResourceGeneric.DATABASE, ORole.PERMISSION_CREATE);
    return (THISDB) super.create();
  }

  @Override
  public <THISDB extends ODatabase> THISDB create(final Map<OGlobalConfiguration, Object> iInitialSettings) {
    checkSecurity(ORule.ResourceGeneric.DATABASE, ORole.PERMISSION_CREATE);
    return (THISDB) super.create(iInitialSettings);
  }

  @Override
  public void drop() {
    checkOpeness();
    checkSecurity(ORule.ResourceGeneric.DATABASE, ORole.PERMISSION_DELETE);
    super.drop();
  }

  @Override
  public int addCluster(String iClusterName, int iRequestedId, Object... iParameters) {
    checkOpeness();
    checkSecurity(ORule.ResourceGeneric.DATABASE, ORole.PERMISSION_UPDATE);
    return super.addCluster(iClusterName, iRequestedId, iParameters);
  }

  @Override
  public int addCluster(String iClusterName, Object... iParameters) {
    checkOpeness();
    checkSecurity(ORule.ResourceGeneric.DATABASE, ORole.PERMISSION_UPDATE);
    return super.addCluster(iClusterName, iParameters);
  }

  @Override
  public boolean dropCluster(final String iClusterName, final boolean iTruncate) {
    checkSecurity(ORule.ResourceGeneric.DATABASE, ORole.PERMISSION_UPDATE);
    checkClusterBoundedToClass(getClusterIdByName(iClusterName));
    return super.dropCluster(iClusterName, iTruncate);
  }

  public boolean dropCluster(int iClusterId, final boolean iTruncate) {
    checkSecurity(ORule.ResourceGeneric.DATABASE, ORole.PERMISSION_UPDATE);
    checkClusterBoundedToClass(iClusterId);
    return super.dropCluster(iClusterId, iTruncate);
  }

  public OBinarySerializerFactory getSerializerFactory() {
    return underlying.getSerializerFactory();
  }

  public OTransaction getTransaction() {
    return underlying.getTransaction();
  }

  public void replaceStorage(OStorage iNewStorage) {
    underlying.replaceStorage(iNewStorage);
  }

  public ODatabase<ORecord> begin() {
    return underlying.begin();
  }

  public ODatabase<ORecord> begin(final TXTYPE iType) {
    return underlying.begin(iType);
  }

  public ODatabase<ORecord> begin(final OTransaction iTx) {
    return underlying.begin(iTx);
  }

  public boolean isMVCC() {
    checkOpeness();
    return underlying.isMVCC();
  }

  public <RET extends ODatabase<?>> RET setMVCC(final boolean iValue) {
    checkOpeness();
    return (RET) underlying.setMVCC(iValue);
  }

  public boolean isValidationEnabled() {
    return underlying.isValidationEnabled();
  }

  public <RET extends ODatabaseDocument> RET setValidationEnabled(final boolean iValue) {
    return (RET) underlying.setValidationEnabled(iValue);
  }

  public OSecurityUser getUser() {
    return underlying.getUser();
  }

  public void setUser(OSecurityUser user) {
    underlying.setUser(user);
  }

  public OMetadata getMetadata() {
    return underlying.getMetadata();
  }

  public ODictionary<ORecord> getDictionary() {
    return underlying.getDictionary();
  }

  public byte getRecordType() {
    return underlying.getRecordType();
  }

  public <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(final String iClusterName) {
    return underlying.browseCluster(iClusterName);
  }

  public <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(final String iClusterName, final Class<REC> iRecordClass) {
    return underlying.browseCluster(iClusterName, iRecordClass);
  }

  public <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(final String iClusterName, final Class<REC> iRecordClass,
      long startClusterPosition, long endClusterPosition, final boolean loadTombstones) {

    return underlying.browseCluster(iClusterName, iRecordClass, startClusterPosition, endClusterPosition, loadTombstones);
  }

  public <RET extends OCommandRequest> RET command(final OCommandRequest iCommand) {
    return (RET) underlying.command(iCommand);
  }

  public <RET extends List<?>> RET query(final OQuery<? extends Object> iCommand, final Object... iArgs) {
    return (RET) underlying.query(iCommand, iArgs);
  }

  public <RET extends Object> RET newInstance() {
    return (RET) underlying.newInstance();
  }

  public ODatabase<ORecord> delete(final ORID iRid) {
    underlying.delete(iRid);
    return this;
  }

  @Override
  public ODatabase<ORecord> delete(final ORID iRid, final ORecordVersion iVersion) {
    underlying.delete(iRid, iVersion);
    return this;
  }

  @Override
  public boolean hide(ORID rid) {
    return underlying.hide(rid);
  }

  @Override
  public ODatabase<ORecord> cleanOutRecord(ORID rid, ORecordVersion version) {
    underlying.cleanOutRecord(rid, version);
    return this;
  }

  public ODatabase<ORecord> delete(final ORecord iRecord) {
    underlying.delete(iRecord);
    return this;
  }

  public <RET extends ORecord> RET load(final ORID recordId) {
    return (RET) underlying.load(recordId);
  }

  public <RET extends ORecord> RET load(final ORID iRecordId, final String iFetchPlan) {
    return (RET) underlying.load(iRecordId, iFetchPlan);
  }

  public <RET extends ORecord> RET load(final ORID iRecordId, final String iFetchPlan, final boolean iIgnoreCache) {
    return (RET) underlying.load(iRecordId, iFetchPlan, iIgnoreCache);
  }

  @Override
  @Deprecated
  public <RET extends ORecord> RET load(ORID iRecordId, String iFetchPlan, boolean iIgnoreCache, boolean loadTombstone,
      OStorage.LOCKING_STRATEGY iLockingStrategy) {
    return (RET) underlying.load(iRecordId, iFetchPlan, iIgnoreCache, loadTombstone, OStorage.LOCKING_STRATEGY.DEFAULT);
  }

  @Override
  @Deprecated
  public <RET extends ORecord> RET load(ORecord iObject, String iFetchPlan, boolean iIgnoreCache, boolean loadTombstone,
      OStorage.LOCKING_STRATEGY iLockingStrategy) {
    return (RET) underlying.load(iObject, iFetchPlan, iIgnoreCache, loadTombstone, OStorage.LOCKING_STRATEGY.DEFAULT);
  }

  public <RET extends ORecord> RET getRecord(final OIdentifiable iIdentifiable) {
    return (RET) underlying.getRecord(iIdentifiable);
  }

  public <RET extends ORecord> RET load(final ORecord iRecord) {
    return (RET) underlying.load(iRecord);
  }

  public <RET extends ORecord> RET load(final ORecord iRecord, final String iFetchPlan) {
    return (RET) underlying.load(iRecord, iFetchPlan);
  }

  public <RET extends ORecord> RET load(final ORecord iRecord, final String iFetchPlan, final boolean iIgnoreCache) {
    return (RET) underlying.load(iRecord, iFetchPlan, iIgnoreCache);
  }

  public <RET extends ORecord> RET reload(final ORecord iRecord) {
    return (RET) underlying.reload(iRecord, null, true);
  }

  public <RET extends ORecord> RET reload(final ORecord iRecord, final String iFetchPlan, final boolean iIgnoreCache) {
    return (RET) underlying.reload(iRecord, iFetchPlan, iIgnoreCache);
  }

  public <RET extends ORecord> RET save(final ORecord iRecord) {
    return (RET) underlying.save(iRecord);
  }

  public <RET extends ORecord> RET save(final ORecord iRecord, final String iClusterName) {
    return (RET) underlying.save(iRecord, iClusterName);
  }

  public <RET extends ORecord> RET save(final ORecord iRecord, final OPERATION_MODE iMode, boolean iForceCreate,
      final ORecordCallback<? extends Number> iRecordCreatedCallback, ORecordCallback<ORecordVersion> iRecordUpdatedCallback) {
    return (RET) underlying.save(iRecord, iMode, iForceCreate, iRecordCreatedCallback, iRecordUpdatedCallback);
  }

  public <RET extends ORecord> RET save(final ORecord iRecord, final String iClusterName, final OPERATION_MODE iMode,
      boolean iForceCreate, final ORecordCallback<? extends Number> iRecordCreatedCallback,
      ORecordCallback<ORecordVersion> iRecordUpdatedCallback) {
    return (RET) underlying.save(iRecord, iClusterName, iMode, iForceCreate, iRecordCreatedCallback, iRecordUpdatedCallback);
  }

  public void setInternal(final ATTRIBUTES attribute, final Object iValue) {
    underlying.setInternal(attribute, iValue);
  }

  public boolean isRetainRecords() {
    return underlying.isRetainRecords();
  }

  public ODatabaseDocument setRetainRecords(boolean iValue) {
    underlying.setRetainRecords(iValue);
    return (ODatabaseDocument) this.getClass().cast(this);
  }

  public ORecord getRecordByUserObject(final Object iUserObject, final boolean iCreateIfNotAvailable) {
    if (databaseOwner != this)
      return getDatabaseOwner().getRecordByUserObject(iUserObject, false);

    return (ORecord) iUserObject;
  }

  public void registerUserObject(final Object iObject, final ORecord iRecord) {
    if (databaseOwner != this)
      getDatabaseOwner().registerUserObject(iObject, iRecord);
  }

  public void registerUserObjectAfterLinkSave(ORecord iRecord) {
    if (databaseOwner != this)
      getDatabaseOwner().registerUserObjectAfterLinkSave(iRecord);
  }

  public Object getUserObjectByRecord(final OIdentifiable iRecord, final String iFetchPlan) {
    if (databaseOwner != this)
      return databaseOwner.getUserObjectByRecord(iRecord, iFetchPlan);

    return iRecord;
  }

  public boolean existsUserObjectByRID(final ORID iRID) {
    if (databaseOwner != this)
      return databaseOwner.existsUserObjectByRID(iRID);
    return false;
  }

  public <DBTYPE extends ODatabaseDocument> DBTYPE checkSecurity(ORule.ResourceGeneric resourceGeneric, String resourceSpecific,
      final int iOperation) {
    return (DBTYPE) underlying.checkSecurity(resourceGeneric, resourceSpecific, iOperation);
  }

  public <DBTYPE extends ODatabaseDocument> DBTYPE checkSecurity(final ORule.ResourceGeneric iResourceGeneric,
      final int iOperation, final Object iResourceSpecific) {
    return (DBTYPE) underlying.checkSecurity(iResourceGeneric, iOperation, iResourceSpecific);
  }

  public <DBTYPE extends ODatabaseDocument> DBTYPE checkSecurity(final ORule.ResourceGeneric iResourceGeneric,
      final int iOperation, final Object... iResourcesSpecific) {
    return (DBTYPE) underlying.checkSecurity(iResourceGeneric, iOperation, iResourcesSpecific);
  }

  public <DBTYPE extends ODatabase<?>> DBTYPE registerHook(final ORecordHook iHookImpl) {
    underlying.registerHook(iHookImpl);
    return (DBTYPE) this;
  }

  public <DBTYPE extends ODatabase<?>> DBTYPE registerHook(final ORecordHook iHookImpl, ORecordHook.HOOK_POSITION iPosition) {
    underlying.registerHook(iHookImpl, iPosition);
    return (DBTYPE) this;
  }

  public RESULT callbackHooks(final TYPE iType, final OIdentifiable iObject) {
    return underlying.callbackHooks(iType, iObject);
  }

  public Map<ORecordHook, ORecordHook.HOOK_POSITION> getHooks() {
    return underlying.getHooks();
  }

  public <DBTYPE extends ODatabase<?>> DBTYPE unregisterHook(final ORecordHook iHookImpl) {
    underlying.unregisterHook(iHookImpl);
    return (DBTYPE) this;
  }

  /**
   * Executes a backup of the database. During the backup the database will be frozen in read-only mode.
   * 
   * @param out
   *          OutputStream used to write the backup content. Use a FileOutputStream to make the backup persistent on disk
   * @param options
   *          Backup options as Map<String, Object> object
   * @param callable
   *          Callback to execute when the database is locked
   * @param iListener
   *          Listener called for backup messages
   * @param compressionLevel
   *          ZIP Compression level between 0 (no compression) and 9 (maximum). The bigger is the compression, the smaller will be
   *          the final backup content, but will consume more CPU and time to execute
   * @param bufferSize
   *          Buffer size in bytes, the bigger is the buffer, the more efficient will be the compression
   * @throws IOException
   */
  @Override
  public void backup(final OutputStream out, final Map<String, Object> options, final Callable<Object> callable,
      final OCommandOutputListener iListener, int compressionLevel, int bufferSize) throws IOException {
    underlying.backup(out, options, new Callable<Object>() {

      @Override
      public Object call() throws Exception {
        // FLUSHES ALL THE INDEX BEFORE
        for (OIndex<?> index : getMetadata().getIndexManager().getIndexes()) {
          index.flush();
        }
        if (callable != null)
          return callable.call();
        return null;
      }
    }, iListener, compressionLevel, bufferSize);
  }

  @Override
  public ORecordConflictStrategy getConflictStrategy() {
    return getStorage().getConflictStrategy();
  }

  @Override
  public ODatabaseRecordWrapperAbstract<DB> setConflictStrategy(final String iStrategyName) {
    getStorage().setConflictStrategy(Orient.instance().getRecordConflictStrategy().getStrategy(iStrategyName));
    return this;
  }

  @Override
  public ODatabaseRecordWrapperAbstract<DB> setConflictStrategy(final ORecordConflictStrategy iResolver) {
    getStorage().setConflictStrategy(iResolver);
    return this;
  }

  @Override
  public void resetInitialization() {
    underlying.resetInitialization();
  }

  @Override
  public ODatabase<ORecord> commit() throws OTransactionException {
    return underlying.commit();
  }

  @Override
  public ODatabase<ORecord> commit(boolean force) throws OTransactionException {
    return underlying.commit(false);
  }

  @Override
  public ODatabase<ORecord> rollback() throws OTransactionException {
    return underlying.rollback();
  }

  @Override
  public ODatabase<ORecord> rollback(boolean force) throws OTransactionException {
    return underlying.rollback(force);
  }

  @Override
  public String getType() {
    return underlying.getType();
  }

  @Override
  public OCurrentStorageComponentsFactory getStorageVersions() {
    return underlying.getStorageVersions();
  }

  @Override
  public OSBTreeCollectionManager getSbTreeCollectionManager() {
    return underlying.getSbTreeCollectionManager();
  }

  @Override
  public ORecordSerializer getSerializer() {
    return underlying.getSerializer();
  }

  @Override
  public ORecordIteratorClass<ODocument> browseClass(String iClassName) {
    return underlying.browseClass(iClassName);
  }

  @Override
  public ORecordIteratorClass<ODocument> browseClass(String iClassName, boolean iPolymorphic) {
    return underlying.browseClass(iClassName, iPolymorphic);
  }

  @Override
  public <REC extends ORecord> ORecordIteratorCluster<REC> browseCluster(String iClusterName, long startClusterPosition,
      long endClusterPosition, boolean loadTombstones) {
    return underlying.browseCluster(iClusterName, startClusterPosition, endClusterPosition, loadTombstones);
  }

  @Override
  public <RET> RET newInstance(String iClassName) {
    return underlying.newInstance(iClassName);
  }

  @Override
  public long countClass(String iClassName) {
    return underlying.countClass(iClassName);
  }

  @Override
  public long countClass(String iClassName, boolean iPolymorphic) {
    return underlying.countClass(iClassName, iPolymorphic);
  }

  protected void checkClusterBoundedToClass(final int iClusterId) {
    if (iClusterId == -1)
      return;

    for (OClass clazz : ((OMetadataInternal) getMetadata()).getImmutableSchemaSnapshot().getClasses()) {
      if (clazz.getDefaultClusterId() == iClusterId)
        throw new OSchemaException("Cannot drop the cluster '" + getClusterNameById(iClusterId) + "' because the classes ['"
            + clazz.getName() + "'] are bound to it. Drop these classes before dropping the cluster");
      else if (clazz.getClusterIds().length > 1) {
        for (int i : clazz.getClusterIds()) {
          if (i == iClusterId)
            throw new OSchemaException("Cannot drop the cluster '" + getClusterNameById(iClusterId) + "' because the classes ['"
                + clazz.getName() + "'] are bound to it. Drop these classes before dropping the cluster");
        }
      }
    }
  }
}
