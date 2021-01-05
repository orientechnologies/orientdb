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
package com.orientechnologies.orient.object.db;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.script.OCommandScriptException;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseWrapperAbstract;
import com.orientechnologies.orient.core.db.OLiveQueryMonitor;
import com.orientechnologies.orient.core.db.OLiveQueryResultListener;
import com.orientechnologies.orient.core.db.OSharedContext;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.entity.OEntityManager;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.OSerializationThreadLocal;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.OStorageInfo;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionNoTx;
import com.orientechnologies.orient.object.dictionary.ODictionaryWrapper;
import com.orientechnologies.orient.object.enhancement.OObjectEntityEnhancer;
import com.orientechnologies.orient.object.enhancement.OObjectEntitySerializer;
import com.orientechnologies.orient.object.enhancement.OObjectMethodFilter;
import com.orientechnologies.orient.object.enhancement.OObjectProxyMethodHandler;
import com.orientechnologies.orient.object.entity.OObjectEntityClassHandler;
import com.orientechnologies.orient.object.iterator.OObjectIteratorClass;
import com.orientechnologies.orient.object.iterator.OObjectIteratorCluster;
import com.orientechnologies.orient.object.metadata.OMetadataObject;
import com.orientechnologies.orient.object.serialization.OObjectSerializerHelper;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyObject;

/**
 * Object Database instance. It's a wrapper to the class ODatabaseDocumentTx that handles conversion
 * between ODocument instances and POJOs using javassist APIs.
 *
 * @author Luca Molino
 * @see ODatabaseDocumentTx
 */
@SuppressWarnings("unchecked")
public class OObjectDatabaseTx extends ODatabaseWrapperAbstract<ODatabaseDocumentInternal, Object>
    implements ODatabaseObject {

  public static final String TYPE = "object";
  protected ODictionary<Object> dictionary;
  protected OEntityManager entityManager;
  protected boolean saveOnlyDirty;
  protected boolean lazyLoading;
  protected boolean automaticSchemaGeneration;
  protected OMetadataObject metadata;

  @Deprecated
  public OObjectDatabaseTx(final String iURL) {
    super(new ODatabaseDocumentTx(iURL));
    underlying.setDatabaseOwner(this);
    init();
  }

  /**
   * Constructor to wrap an existing database connect for object connections
   *
   * @param iDatabase an open database connection
   */
  public OObjectDatabaseTx(ODatabaseDocumentInternal iDatabase) {
    super(iDatabase);
    underlying.setDatabaseOwner(this);
    init();
  }

  public <T> T newInstance(final Class<T> iType) {
    return (T) newInstance(iType.getSimpleName(), null, OCommonConst.EMPTY_OBJECT_ARRAY);
  }

  public <T> T newInstance(final Class<T> iType, Object... iArgs) {
    return (T) newInstance(iType.getSimpleName(), null, iArgs);
  }

  public <RET> RET newInstance(String iClassName) {
    return (RET) newInstance(iClassName, null, OCommonConst.EMPTY_OBJECT_ARRAY);
  }

  @Override
  public <THISDB extends ODatabase> THISDB open(String iUserName, String iUserPassword) {
    super.open(iUserName, iUserPassword);
    saveOnlyDirty =
        getConfiguration().getValueAsBoolean(OGlobalConfiguration.OBJECT_SAVE_ONLY_DIRTY);

    entityManager.registerEntityClass(OUser.class);
    entityManager.registerEntityClass(ORole.class);
    metadata = new OMetadataObject((OMetadataInternal) underlying.getMetadata(), underlying);
    this.registerFieldMappingStrategy();
    return (THISDB) this;
  }

  @Override
  public <THISDB extends ODatabase> THISDB open(OToken iToken) {
    super.open(iToken);
    entityManager.registerEntityClass(OUser.class);
    entityManager.registerEntityClass(ORole.class);
    metadata = new OMetadataObject((OMetadataInternal) underlying.getMetadata(), underlying);
    this.registerFieldMappingStrategy();
    return (THISDB) this;
  }

  public OSecurityUser getUser() {
    return underlying.getUser();
  }

  public void setUser(OSecurityUser user) {
    underlying.setUser(user);
  }

  @Override
  public OMetadataObject getMetadata() {
    checkOpenness();
    if (metadata == null)
      metadata = new OMetadataObject((OMetadataInternal) underlying.getMetadata(), underlying);
    return metadata;
  }

  public void setInternal(final ATTRIBUTES attribute, final Object iValue) {
    underlying.setInternal(attribute, iValue);
  }

  @Override
  public Iterable<ODatabaseListener> getListeners() {
    return underlying.getListeners();
  }

  public <DBTYPE extends ODatabase<?>> DBTYPE registerHook(final ORecordHook iHookImpl) {
    underlying.registerHook(iHookImpl);
    return (DBTYPE) this;
  }

  public <DBTYPE extends ODatabase<?>> DBTYPE registerHook(
      final ORecordHook iHookImpl, ORecordHook.HOOK_POSITION iPosition) {
    underlying.registerHook(iHookImpl, iPosition);
    return (DBTYPE) this;
  }

  public ORecordHook.RESULT callbackHooks(
      final ORecordHook.TYPE iType, final OIdentifiable iObject) {
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
   * Create a new POJO by its class name. Assure to have called the registerEntityClasses()
   * declaring the packages that are part of entity classes.
   *
   * @see OEntityManager#registerEntityClasses(String)
   */
  public <RET extends Object> RET newInstance(
      final String iClassName, final Object iEnclosingClass, Object... iArgs) {
    underlying.checkIfActive();

    checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_CREATE, iClassName);

    try {
      Class<?> entityClass = entityManager.getEntityClass(iClassName);
      if (entityClass != null) {
        RET enhanced =
            (RET)
                OObjectEntityEnhancer.getInstance()
                    .getProxiedInstance(
                        entityManager.getEntityClass(iClassName),
                        iEnclosingClass,
                        underlying.newInstance(iClassName),
                        null,
                        iArgs);
        return (RET) enhanced;
      } else {
        throw new OSerializationException(
            "Type "
                + iClassName
                + " cannot be serialized because is not part of registered entities. To fix this error register this class");
      }
    } catch (Exception e) {
      final String message = "Error on creating object of class " + iClassName;
      OLogManager.instance().error(this, message, e);

      throw OException.wrapException(new ODatabaseException(message), e);
    }
  }

  /**
   * Create a new POJO by its class name. Assure to have called the registerEntityClasses()
   * declaring the packages that are part of entity classes.
   *
   * @see OEntityManager#registerEntityClasses(String)
   */
  public <RET extends Object> RET newInstance(
      final String iClassName, final Object iEnclosingClass, ODocument iDocument, Object... iArgs) {
    checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_CREATE, iClassName);

    try {
      Class<?> entityClass = entityManager.getEntityClass(iClassName);
      if (entityClass != null) {
        RET enhanced =
            (RET)
                OObjectEntityEnhancer.getInstance()
                    .getProxiedInstance(
                        entityManager.getEntityClass(iClassName),
                        iEnclosingClass,
                        iDocument,
                        null,
                        iArgs);
        return (RET) enhanced;
      } else {
        throw new OSerializationException(
            "Type "
                + iClassName
                + " cannot be serialized because is not part of registered entities. To fix this error register this class");
      }
    } catch (Exception e) {
      final String message = "Error on creating object of class " + iClassName;
      OLogManager.instance().error(this, message, e);

      throw OException.wrapException(new ODatabaseException(message), e);
    }
  }

  public <RET> OObjectIteratorClass<RET> browseClass(final Class<RET> iClusterClass) {
    return browseClass(iClusterClass, true);
  }

  public <RET> OObjectIteratorClass<RET> browseClass(
      final Class<RET> iClusterClass, final boolean iPolymorphic) {
    if (iClusterClass == null) return null;

    return browseClass(iClusterClass.getSimpleName(), iPolymorphic);
  }

  public <RET> OObjectIteratorClass<RET> browseClass(final String iClassName) {
    return browseClass(iClassName, true);
  }

  public <RET> OObjectIteratorClass<RET> browseClass(
      final String iClassName, final boolean iPolymorphic) {
    checkOpenness();
    checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_READ, iClassName);

    return new OObjectIteratorClass<RET>(this, getUnderlying(), iClassName, iPolymorphic);
  }

  public <RET> OObjectIteratorCluster<RET> browseCluster(final String iClusterName) {
    checkOpenness();
    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, iClusterName);

    return (OObjectIteratorCluster<RET>)
        new OObjectIteratorCluster<Object>(this, getUnderlying(), getClusterIdByName(iClusterName));
  }

  public <RET> RET load(final Object iPojo) {
    return (RET) load(iPojo, null);
  }

  public <RET> RET reload(final Object iPojo) {
    return (RET) reload(iPojo, null, true);
  }

  public <RET> RET reload(final Object iPojo, final boolean iIgnoreCache) {
    return (RET) reload(iPojo, null, iIgnoreCache);
  }

  public <RET> RET reload(Object iPojo, final String iFetchPlan, final boolean iIgnoreCache) {
    return reload(iPojo, iFetchPlan, iIgnoreCache, true);
  }

  @Override
  public <RET> RET reload(Object iObject, String iFetchPlan, boolean iIgnoreCache, boolean force) {
    checkOpenness();
    if (iObject == null) return null;

    // GET THE ASSOCIATED DOCUMENT
    final ODocument record = getRecordByUserObject(iObject, true);
    underlying.reload(record, iFetchPlan, iIgnoreCache, force);

    iObject = stream2pojo(record, iObject, iFetchPlan, true);
    return (RET) iObject;
  }

  public <RET> RET load(final Object iPojo, final String iFetchPlan) {
    return (RET) load(iPojo, iFetchPlan, false);
  }

  public void attach(final Object iPojo) {
    OObjectEntitySerializer.attach(iPojo, this);
  }

  public <RET> RET attachAndSave(final Object iPojo) {
    attach(iPojo);
    return (RET) save(iPojo);
  }

  /**
   * Method that detaches all fields contained in the document to the given object. It returns by
   * default a proxied instance.
   *
   * @param iPojo :- the object to detach
   * @return the detached object
   */
  public <RET> RET detach(final Object iPojo) {
    return (RET) OObjectEntitySerializer.detach(iPojo, this);
  }

  /**
   * Method that detaches all fields contained in the document to the given object.
   *
   * @param <RET>
   * @param iPojo :- the object to detach
   * @param returnNonProxiedInstance :- defines if the return object will be a proxied instance or
   *     not. If set to TRUE and the object does not contains @Id and @Version fields it could
   *     procude data replication
   * @return the object serialized or with detached data
   */
  public <RET> RET detach(final Object iPojo, boolean returnNonProxiedInstance) {
    return (RET) OObjectEntitySerializer.detach(iPojo, this, returnNonProxiedInstance);
  }

  /**
   * Method that detaches all fields contained in the document to the given object and recursively
   * all object tree. This may throw a {@link StackOverflowError} with big objects tree. To avoid it
   * set the stack size with -Xss java option
   *
   * @param <RET>
   * @param iPojo :- the object to detach
   * @param returnNonProxiedInstance :- defines if the return object will be a proxied instance or
   *     not. If set to TRUE and the object does not contains @Id and @Version fields it could
   *     procude data replication
   * @return the object serialized or with detached data
   */
  public <RET> RET detachAll(final Object iPojo, boolean returnNonProxiedInstance) {
    return detachAll(
        iPojo,
        returnNonProxiedInstance,
        new HashMap<Object, Object>(),
        new HashMap<Object, Object>());
  }

  @Override
  public <RET> RET load(Object iPojo, String iFetchPlan, boolean iIgnoreCache) {
    checkOpenness();
    if (iPojo == null) return null;

    // GET THE ASSOCIATED DOCUMENT
    ODocument record = getRecordByUserObject(iPojo, true);
    try {
      record.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);

      record = underlying.load(record, iFetchPlan, iIgnoreCache);

      return (RET) stream2pojo(record, iPojo, iFetchPlan);
    } finally {
      record.setInternalStatus(ORecordElement.STATUS.LOADED);
    }
  }

  @Override
  public <RET> RET lock(ORID recordId) throws OLockException {
    checkOpenness();
    if (recordId == null) return null;

    // GET THE ASSOCIATED DOCUMENT
    final ODocument record = (ODocument) underlying.lock(recordId);
    if (record == null) return null;

    return (RET)
        OObjectEntityEnhancer.getInstance()
            .getProxiedInstance(record.getClassName(), entityManager, record, null);
  }

  @Override
  public <RET> RET lock(ORID recordId, long timeout, TimeUnit timeoutUnit) throws OLockException {
    checkOpenness();
    if (recordId == null) return null;

    // GET THE ASSOCIATED DOCUMENT
    final ODocument record = (ODocument) underlying.lock(recordId, timeout, timeoutUnit);
    if (record == null) return null;

    return (RET)
        OObjectEntityEnhancer.getInstance()
            .getProxiedInstance(record.getClassName(), entityManager, record, null);
  }

  @Override
  public void unlock(ORID recordId) throws OLockException {
    checkOpenness();
    underlying.unlock(recordId);
  }

  public <RET> RET load(final ORID recordId) {
    return (RET) load(recordId, null);
  }

  public <RET> RET load(final ORID iRecordId, final String iFetchPlan) {
    return (RET) load(iRecordId, iFetchPlan, false);
  }

  @Override
  public <RET> RET load(ORID iRecordId, String iFetchPlan, boolean iIgnoreCache) {
    checkOpenness();
    if (iRecordId == null) return null;

    // GET THE ASSOCIATED DOCUMENT
    final ODocument record = (ODocument) underlying.load(iRecordId, iFetchPlan, iIgnoreCache);
    if (record == null) return null;

    return (RET)
        OObjectEntityEnhancer.getInstance()
            .getProxiedInstance(record.getClassName(), entityManager, record, null);
  }

  /**
   * Saves an object to the databasein synchronous mode . First checks if the object is new or not.
   * In case it's new a new ODocument is created and bound to the object, otherwise the ODocument is
   * retrieved and updated. The object is introspected using the Java Reflection to extract the
   * field values. <br>
   * If a multi value (array, collection or map of objects) is passed, then each single object is
   * stored separately.
   */
  public <RET> RET save(final Object iContent) {
    return (RET) save(iContent, (String) null, OPERATION_MODE.SYNCHRONOUS, false, null, null);
  }

  /**
   * Saves an object to the database specifying the mode. First checks if the object is new or not.
   * In case it's new a new ODocument is created and bound to the object, otherwise the ODocument is
   * retrieved and updated. The object is introspected using the Java Reflection to extract the
   * field values. <br>
   * If a multi value (array, collection or map of objects) is passed, then each single object is
   * stored separately.
   */
  public <RET> RET save(
      final Object iContent,
      OPERATION_MODE iMode,
      boolean iForceCreate,
      final ORecordCallback<? extends Number> iRecordCreatedCallback,
      ORecordCallback<Integer> iRecordUpdatedCallback) {
    return (RET) save(iContent, null, iMode, false, iRecordCreatedCallback, iRecordUpdatedCallback);
  }

  /**
   * Saves an object in synchronous mode to the database forcing a record cluster where to store it.
   * First checks if the object is new or not. In case it's new a new ODocument is created and bound
   * to the object, otherwise the ODocument is retrieved and updated. The object is introspected
   * using the Java Reflection to extract the field values. <br>
   * If a multi value (array, collection or map of objects) is passed, then each single object is
   * stored separately.
   *
   * <p>Before to use the specified cluster a check is made to know if is allowed and figures in the
   * configured and the record is valid following the constraints declared in the schema.
   *
   * @see ODocument#validate()
   */
  public <RET> RET save(final Object iPojo, final String iClusterName) {
    return (RET) save(iPojo, iClusterName, OPERATION_MODE.SYNCHRONOUS, false, null, null);
  }

  /**
   * Saves an object to the database forcing a record cluster where to store it. First checks if the
   * object is new or not. In case it's new a new ODocument is created and bound to the object,
   * otherwise the ODocument is retrieved and updated. The object is introspected using the Java
   * Reflection to extract the field values. <br>
   * If a multi value (array, collection or map of objects) is passed, then each single object is
   * stored separately.
   *
   * <p>Before to use the specified cluster a check is made to know if is allowed and figures in the
   * configured and the record is valid following the constraints declared in the schema.
   *
   * @see ODocument#validate()
   */
  public <RET> RET save(
      final Object iPojo,
      final String iClusterName,
      OPERATION_MODE iMode,
      boolean iForceCreate,
      final ORecordCallback<? extends Number> iRecordCreatedCallback,
      ORecordCallback<Integer> iRecordUpdatedCallback) {
    checkOpenness();
    if (iPojo == null) return (RET) iPojo;
    else if (OMultiValue.isMultiValue(iPojo)) {
      // MULTI VALUE OBJECT: STORE SINGLE POJOS
      for (Object pojo : OMultiValue.getMultiValueIterable(iPojo)) {
        save(pojo, iClusterName);
      }
      return (RET) iPojo;
    } else {
      OSerializationThreadLocal.INSTANCE.get().clear();

      // GET THE ASSOCIATED DOCUMENT
      final Object proxiedObject = OObjectEntitySerializer.serializeObject(iPojo, this);
      final ODocument record = getRecordByUserObject(proxiedObject, true);
      try {
        record.setInternalStatus(ORecordElement.STATUS.MARSHALLING);

        if (!saveOnlyDirty || record.isDirty()) {
          // REGISTER BEFORE TO SERIALIZE TO AVOID PROBLEMS WITH CIRCULAR DEPENDENCY
          // registerUserObject(iPojo, record);
          deleteOrphans((((OObjectProxyMethodHandler) ((ProxyObject) proxiedObject).getHandler())));

          ODocument savedRecord =
              underlying.save(
                  record,
                  iClusterName,
                  iMode,
                  iForceCreate,
                  iRecordCreatedCallback,
                  iRecordUpdatedCallback);

          ((OObjectProxyMethodHandler) ((ProxyObject) proxiedObject).getHandler())
              .setDoc(savedRecord);
          ((OObjectProxyMethodHandler) ((ProxyObject) proxiedObject).getHandler())
              .updateLoadedFieldMap(proxiedObject, false);
          // RE-REGISTER FOR NEW RECORDS SINCE THE ID HAS CHANGED
          registerUserObject(proxiedObject, record);
        }
      } finally {
        record.setInternalStatus(ORecordElement.STATUS.LOADED);
      }
      return (RET) proxiedObject;
    }
  }

  public ODatabaseObject delete(final Object iPojo) {
    checkOpenness();

    if (iPojo == null) return this;

    ODocument record = getRecordByUserObject(iPojo, false);
    if (record == null) {
      final ORecordId rid = OObjectSerializerHelper.getObjectID(this, iPojo);
      if (rid == null)
        throw new OObjectNotDetachedException(
            "Cannot retrieve the object's ID for '" + iPojo + "' because has not been detached");

      record = (ODocument) underlying.load(rid);
    }
    deleteCascade(record);

    underlying.delete(record);

    if (getTransaction() instanceof OTransactionNoTx) unregisterPojo(iPojo, record);

    return this;
  }

  @Override
  public ODatabaseObject delete(final ORID iRID) {
    checkOpenness();

    if (iRID == null) return this;

    final ORecord record = iRID.getRecord();
    if (record instanceof ODocument) {
      Object iPojo = getUserObjectByRecord(record, null);

      deleteCascade((ODocument) record);

      underlying.delete(record);

      if (getTransaction() instanceof OTransactionNoTx) unregisterPojo(iPojo, (ODocument) record);
    }
    return this;
  }

  @Override
  public ODatabaseObject delete(final ORID iRID, final int iVersion) {
    deleteRecord(iRID, iVersion, false);
    return this;
  }

  public ODatabaseObject delete(final ORecord iRecord) {
    underlying.delete(iRecord);
    return this;
  }

  public long countClass(final String iClassName) {
    checkOpenness();
    return underlying.countClass(iClassName);
  }

  public long countClass(final String iClassName, final boolean iPolymorphic) {
    checkOpenness();
    return underlying.countClass(iClassName, iPolymorphic);
  }

  public long countClass(final Class<?> iClass) {
    checkOpenness();
    return underlying.countClass(iClass.getSimpleName());
  }

  /** {@inheritDoc} */
  @Deprecated
  public ODictionary<Object> getDictionary() {
    checkOpenness();
    if (dictionary == null)
      dictionary = new ODictionaryWrapper(this, underlying.getDictionary().getIndex());

    return dictionary;
  }

  public OTransaction getTransaction() {
    return underlying.getTransaction();
  }

  public OObjectDatabaseTx begin() {
    underlying.begin();
    return this;
  }

  public OObjectDatabaseTx begin(final OTransaction.TXTYPE iType) {
    underlying.begin(iType);
    return this;
  }

  public OObjectDatabaseTx begin(final OTransaction iTx) {
    underlying.begin(iTx);
    return this;
  }

  @Override
  public OObjectDatabaseTx commit() {
    // BY PASS DOCUMENT DB
    return commit(false);
  }

  @Override
  public OObjectDatabaseTx commit(boolean force) throws OTransactionException {
    underlying.commit(force);

    if (getTransaction().isActive()) return this;

    if (getTransaction().getRecordOperations() != null) {
      // UPDATE ID & VERSION FOR ALL THE RECORDS
      Object pojo = null;
      for (ORecordOperation entry : getTransaction().getRecordOperations()) {
        switch (entry.type) {
          case ORecordOperation.CREATED:
          case ORecordOperation.UPDATED:
            break;

          case ORecordOperation.DELETED:
            final ORecord rec = entry.getRecord();
            if (rec instanceof ODocument) unregisterPojo(pojo, (ODocument) rec);
            break;
        }
      }
    }

    return this;
  }

  @Override
  public OObjectDatabaseTx rollback() {
    return rollback(false);
  }

  @Override
  public OObjectDatabaseTx rollback(final boolean force) throws OTransactionException {
    // BYPASS DOCUMENT DB
    underlying.rollback(force);

    if (!underlying.getTransaction().isActive()) {
      // COPY ALL TX ENTRIES
      if (getTransaction().getRecordOperations() != null) {
        final List<ORecordOperation> newEntries = new ArrayList<>();
        for (ORecordOperation entry : getTransaction().getRecordOperations())
          if (entry.type == ORecordOperation.CREATED) newEntries.add(entry);
      }
    }
    return this;
  }

  public OEntityManager getEntityManager() {
    return entityManager;
  }

  @Override
  public ODatabaseDocumentInternal getUnderlying() {
    return underlying;
  }

  /**
   * Returns the version number of the object. Version starts from 0 assigned on creation.
   *
   * @param iPojo User object
   */
  public int getVersion(final Object iPojo) {
    checkOpenness();
    final ODocument record = getRecordByUserObject(iPojo, false);
    if (record != null) return record.getVersion();

    return OObjectSerializerHelper.getObjectVersion(iPojo);
  }

  /**
   * Returns the object unique identity.
   *
   * @param iPojo User object
   */
  public ORID getIdentity(final Object iPojo) {
    checkOpenness();
    if (iPojo instanceof OIdentifiable) return ((OIdentifiable) iPojo).getIdentity();
    final ODocument record = getRecordByUserObject(iPojo, false);
    if (record != null) return record.getIdentity();
    return OObjectSerializerHelper.getObjectID(this, iPojo);
  }

  public boolean isSaveOnlyDirty() {
    return saveOnlyDirty;
  }

  public void setSaveOnlyDirty(boolean saveOnlyDirty) {
    this.saveOnlyDirty = saveOnlyDirty;
  }

  public boolean isAutomaticSchemaGeneration() {
    return automaticSchemaGeneration;
  }

  public void setAutomaticSchemaGeneration(boolean automaticSchemaGeneration) {
    this.automaticSchemaGeneration = automaticSchemaGeneration;
  }

  public Object newInstance() {
    checkOpenness();
    return new ODocument();
  }

  public <DBTYPE extends ODatabase> DBTYPE checkSecurity(
      ORule.ResourceGeneric resourceGeneric, String resourceSpecific, final byte iOperation) {
    return (DBTYPE) underlying.checkSecurity(resourceGeneric, resourceSpecific, iOperation);
  }

  public <DBTYPE extends ODatabase> DBTYPE checkSecurity(
      final ORule.ResourceGeneric iResource, final int iOperation, Object iResourceSpecific) {
    return (DBTYPE) underlying.checkSecurity(iResource, iOperation, iResourceSpecific);
  }

  public <DBTYPE extends ODatabase> DBTYPE checkSecurity(
      final ORule.ResourceGeneric iResource, final int iOperation, Object... iResourcesSpecific) {
    return (DBTYPE) underlying.checkSecurity(iResource, iOperation, iResourcesSpecific);
  }

  @Override
  public ODocument pojo2Stream(final Object iPojo, final ODocument iRecord) {
    if (iPojo instanceof ProxyObject) {
      return ((OObjectProxyMethodHandler) ((ProxyObject) iPojo).getHandler()).getDoc();
    }
    return OObjectSerializerHelper.toStream(
        iPojo,
        iRecord,
        getEntityManager(),
        getMetadata().getSchema().getClass(iPojo.getClass().getSimpleName()),
        this,
        this,
        saveOnlyDirty);
  }

  @Override
  public Object stream2pojo(ODocument iRecord, final Object iPojo, final String iFetchPlan) {
    return stream2pojo(iRecord, iPojo, iFetchPlan, false);
  }

  public Object stream2pojo(
      ODocument iRecord, final Object iPojo, final String iFetchPlan, boolean iReload) {
    if (iRecord.getInternalStatus() == ORecordElement.STATUS.NOT_LOADED)
      iRecord = (ODocument) iRecord.load();
    if (iReload) {
      if (iPojo != null) {
        if (iPojo instanceof Proxy) {
          ((OObjectProxyMethodHandler) ((ProxyObject) iPojo).getHandler()).setDoc(iRecord);
          ((OObjectProxyMethodHandler) ((ProxyObject) iPojo).getHandler())
              .updateLoadedFieldMap(iPojo, iReload);
          return iPojo;
        } else
          return OObjectEntityEnhancer.getInstance().getProxiedInstance(iPojo.getClass(), iRecord);
      } else
        return OObjectEntityEnhancer.getInstance()
            .getProxiedInstance(iRecord.getClassName(), entityManager, iRecord, null);
    } else if (!(iPojo instanceof Proxy))
      return OObjectEntityEnhancer.getInstance().getProxiedInstance(iPojo.getClass(), iRecord);
    else return iPojo;
  }

  public boolean isLazyLoading() {
    return lazyLoading;
  }

  public void setLazyLoading(final boolean lazyLoading) {
    this.lazyLoading = lazyLoading;
  }

  public String getType() {
    return TYPE;
  }

  @Override
  public ORecordConflictStrategy getConflictStrategy() {
    return underlying.getConflictStrategy();
  }

  @Override
  public OObjectDatabaseTx setConflictStrategy(final ORecordConflictStrategy iResolver) {
    underlying.setConflictStrategy(iResolver);
    return this;
  }

  @Override
  public OObjectDatabaseTx setConflictStrategy(final String iStrategyName) {
    getStorage()
        .setConflictStrategy(
            Orient.instance().getRecordConflictStrategy().getStrategy(iStrategyName));
    return this;
  }

  @Override
  public ODocument getRecordByUserObject(final Object iPojo, final boolean iCreateIfNotAvailable) {
    if (iPojo instanceof Proxy) return OObjectEntitySerializer.getDocument((Proxy) iPojo);
    return OObjectEntitySerializer.getDocument(
        (Proxy) OObjectEntitySerializer.serializeObject(iPojo, this));
  }

  public Object getUserObjectByRecord(final OIdentifiable iRecord, final String iFetchPlan) {
    return getUserObjectByRecord(iRecord, iFetchPlan, true);
  }

  public Object getUserObjectByRecord(
      final OIdentifiable iRecord, final String iFetchPlan, final boolean iCreate) {
    final ODocument document = iRecord.getRecord();

    return OObjectEntityEnhancer.getInstance()
        .getProxiedInstance(document.getClassName(), getEntityManager(), document, null);
  }

  @Override
  public void registerUserObject(final Object iObject, final ORecord iRecord) {}

  public void registerUserObjectAfterLinkSave(ORecord iRecord) {}

  public void unregisterPojo(final Object iObject, final ODocument iRecord) {}

  public boolean existsUserObjectByRID(ORID iRID) {
    return false;
  }

  public boolean isManaged(final Object iEntity) {
    return false;
  }

  public void registerClassMethodFilter(Class<?> iClass, OObjectMethodFilter iMethodFilter) {
    OObjectEntityEnhancer.getInstance().registerClassMethodFilter(iClass, iMethodFilter);
  }

  public void deregisterClassMethodFilter(final Class<?> iClass) {
    OObjectEntityEnhancer.getInstance().deregisterClassMethodFilter(iClass);
  }

  @Override
  public String incrementalBackup(String path) {
    return underlying.incrementalBackup(path);
  }

  @Override
  public void resetInitialization() {
    underlying.resetInitialization();
  }

  protected <RET> RET detachAll(
      final Object iPojo,
      boolean returnNonProxiedInstance,
      Map<Object, Object> alreadyDetached,
      Map<Object, Object> lazyObjects) {
    return (RET)
        OObjectEntitySerializer.detachAll(
            iPojo, this, returnNonProxiedInstance, alreadyDetached, lazyObjects);
  }

  protected void deleteCascade(final ODocument record) {
    if (record == null) return;
    List<String> toDeleteCascade =
        OObjectEntitySerializer.getCascadeDeleteFields(record.getClassName());
    if (toDeleteCascade != null) {
      for (String field : toDeleteCascade) {
        Object toDelete = record.field(field);
        if (toDelete instanceof OIdentifiable) {
          if (toDelete != null) delete(((OIdentifiable) toDelete).getIdentity());
        } else if (toDelete instanceof Collection) {
          for (OIdentifiable cascadeRecord : ((Collection<OIdentifiable>) toDelete)) {
            if (cascadeRecord != null) delete(((OIdentifiable) cascadeRecord).getIdentity());
          }
        } else if (toDelete instanceof Map) {
          for (OIdentifiable cascadeRecord : ((Map<Object, OIdentifiable>) toDelete).values()) {
            if (cascadeRecord != null) delete(((OIdentifiable) cascadeRecord).getIdentity());
          }
        }
      }
    }
  }

  protected void init() {
    entityManager = OEntityManager.getEntityManagerByDatabaseURL(getURL());
    entityManager.setClassHandler(OObjectEntityClassHandler.getInstance(getURL()));

    lazyLoading = true;
    if (!isClosed() && entityManager.getEntityClass(OUser.class.getSimpleName()) == null) {
      entityManager.registerEntityClass(OUser.class);
      entityManager.registerEntityClass(ORole.class);
    }
  }

  protected void deleteOrphans(final OObjectProxyMethodHandler handler) {
    for (ORID orphan : handler.getOrphans()) {
      final ODocument doc = orphan.getRecord();
      deleteCascade(doc);
      if (doc != null) underlying.delete(doc);
    }
    handler.getOrphans().clear();
  }

  private boolean deleteRecord(ORID iRID, final int iVersion, boolean prohibitTombstones) {
    checkOpenness();

    if (iRID == null) return true;

    ODocument record = iRID.getRecord();
    if (record != null) {
      Object iPojo = getUserObjectByRecord(record, null);

      deleteCascade(record);

      if (prohibitTombstones) underlying.cleanOutRecord(iRID, iVersion);
      else underlying.delete(iRID, iVersion);

      if (getTransaction() instanceof OTransactionNoTx) unregisterPojo(iPojo, record);
    }
    return false;
  }

  @Override
  public int addBlobCluster(String iClusterName, Object... iParameters) {
    return getUnderlying().addBlobCluster(iClusterName, iParameters);
  }

  @Override
  public Set<Integer> getBlobClusterIds() {
    return getUnderlying().getBlobClusterIds();
  }

  @Override
  public OSharedContext getSharedContext() {
    return underlying.getSharedContext();
  }

  /**
   * Register the static document binary mapping mode in the database context (only if it's not
   * already set)
   */
  private void registerFieldMappingStrategy() {
    if (!this.getConfiguration()
        .getContextKeys()
        .contains(OGlobalConfiguration.DOCUMENT_BINARY_MAPPING.getKey())) {
      this.getConfiguration()
          .setValue(
              OGlobalConfiguration.DOCUMENT_BINARY_MAPPING,
              OGlobalConfiguration.DOCUMENT_BINARY_MAPPING.getValueAsInteger());
    }
  }

  /**
   * Returns a wrapped OCommandRequest instance to catch the result-set by converting it before to
   * return to the user application.
   */
  public <RET extends OCommandRequest> RET command(final OCommandRequest iCommand) {
    return (RET) new OCommandSQLPojoWrapper(this, underlying.command(iCommand));
  }

  @Override
  public <RET extends List<?>> RET query(OQuery<?> iCommand, Object... iArgs) {
    checkOpenness();

    convertParameters(iArgs);

    final List<ODocument> result = underlying.query(iCommand, iArgs);

    if (result == null) return null;

    final List<Object> resultPojo = new ArrayList<Object>();
    Object obj;
    for (OIdentifiable doc : result) {
      if (doc instanceof ODocument) {
        // GET THE ASSOCIATED DOCUMENT
        if (((ODocument) doc).getClassName() == null) obj = doc;
        else obj = getUserObjectByRecord(((ODocument) doc), iCommand.getFetchPlan(), true);

        resultPojo.add(obj);
      } else {
        resultPojo.add(doc);
      }
    }

    return (RET) resultPojo;
  }

  /**
   * Converts an array of parameters: if a POJO is used, then replace it with its record id.
   *
   * @param iArgs Array of parameters as Object
   * @see #convertParameter(Object)
   */
  protected void convertParameters(final Object... iArgs) {
    if (iArgs == null) return;

    // FILTER PARAMETERS
    for (int i = 0; i < iArgs.length; ++i) iArgs[i] = convertParameter(iArgs[i]);
  }

  /**
   * Sets as dirty a POJO. This is useful when you change the object and need to tell to the engine
   * to treat as dirty.
   *
   * @param iPojo User object
   */
  public void setDirty(final Object iPojo) {
    if (iPojo == null) return;

    final ODocument record = getRecordByUserObject(iPojo, false);
    if (record == null)
      throw new OObjectNotManagedException(
          "The object " + iPojo + " is not managed by current database");

    record.setDirty();
  }

  /**
   * Sets as not dirty a POJO. This is useful when you change some other object and need to tell to
   * the engine to treat this one as not dirty.
   *
   * @param iPojo User object
   */
  public void unsetDirty(final Object iPojo) {
    if (iPojo == null) return;

    final ODocument record = getRecordByUserObject(iPojo, false);
    if (record == null) return;

    ORecordInternal.unsetDirty(record);
  }

  /**
   * Convert a parameter: if a POJO is used, then replace it with its record id.
   *
   * @param iParameter Parameter to convert, if applicable
   * @see #convertParameters(Object...)
   */
  protected Object convertParameter(final Object iParameter) {
    if (iParameter != null)
      // FILTER PARAMETERS
      if (iParameter instanceof Map<?, ?>) {
        Map<String, Object> map = (Map<String, Object>) iParameter;

        for (Map.Entry<String, Object> e : map.entrySet()) {
          map.put(e.getKey(), convertParameter(e.getValue()));
        }

        return map;
      } else if (iParameter instanceof Collection<?>) {
        List<Object> result = new ArrayList<Object>();
        for (Object object : (Collection<Object>) iParameter) {
          result.add(convertParameter(object));
        }
        return result;
      } else if (iParameter.getClass().isEnum()) {
        return ((Enum<?>) iParameter).name();
      } else if (!OType.isSimpleType(iParameter)) {
        final ORID rid = getIdentity(iParameter);
        if (rid != null && rid.isValid())
          // REPLACE OBJECT INSTANCE WITH ITS RECORD ID
          return rid;
      }

    return iParameter;
  }

  @Deprecated
  public boolean isMVCC() {
    return underlying.isMVCC();
  }

  @Deprecated
  public <DBTYPE extends ODatabase<?>> DBTYPE setMVCC(final boolean iMvcc) {
    underlying.setMVCC(iMvcc);
    return (DBTYPE) this;
  }

  /**
   * Returns true if current configuration retains objects, otherwise false
   *
   * @see #setRetainObjects(boolean)
   */
  @Deprecated
  public boolean isRetainObjects() {
    return false;
  }

  /**
   * Specifies if retain handled objects in memory or not. Setting it to false can improve
   * performance on large inserts. Default is enabled.
   *
   * @param iValue True to enable, false to disable it.
   * @see #isRetainObjects()
   */
  @Deprecated
  public OObjectDatabaseTx setRetainObjects(final boolean iValue) {
    return this;
  }

  @Override
  public OLiveQueryMonitor live(String query, OLiveQueryResultListener listener, Object... args) {
    return underlying.live(query, listener, args);
  }

  @Override
  public OLiveQueryMonitor live(
      String query, OLiveQueryResultListener listener, Map<String, ?> args) {
    return underlying.live(query, listener, args);
  }

  @Override
  public OResultSet query(String query, Object... args)
      throws OCommandSQLParsingException, OCommandExecutionException {
    return underlying.query(query, args);
  }

  @Override
  public OResultSet query(String query, Map args)
      throws OCommandSQLParsingException, OCommandExecutionException {
    return underlying.query(query, args);
  }

  @Override
  public OResultSet command(String query, Object... args)
      throws OCommandSQLParsingException, OCommandExecutionException {
    return underlying.command(query, args);
  }

  public <RET extends List<?>> RET objectQuery(String iCommand, Object... iArgs) {
    checkOpenness();

    convertParameters(iArgs);

    final OResultSet result = underlying.query(iCommand, iArgs);

    if (result == null) return null;

    try {

      final List<Object> resultPojo = new ArrayList<Object>();
      Object obj;
      while (result.hasNext()) {
        OResult doc = result.next();
        if (doc.isElement()) {
          // GET THE ASSOCIATED DOCUMENT
          OElement elem = doc.getElement().get();
          if (elem.getSchemaType().isPresent()) obj = getUserObjectByRecord(elem, null, true);
          else obj = elem;

          resultPojo.add(obj);
        } else {
          resultPojo.add(doc);
        }
      }

      return (RET) resultPojo;
    } finally {
      result.close();
    }
  }

  public <RET extends List<?>> RET objectQuery(String iCommand, Map<String, Object> iArgs) {
    checkOpenness();

    convertParameters(iArgs);

    final OResultSet result = underlying.query(iCommand, iArgs);

    if (result == null) return null;

    try {

      final List<Object> resultPojo = new ArrayList<Object>();
      Object obj;
      while (result.hasNext()) {
        OResult doc = result.next();
        if (doc.isElement()) {
          // GET THE ASSOCIATED DOCUMENT
          OElement elem = doc.getElement().get();
          if (elem.getSchemaType().isPresent()) obj = getUserObjectByRecord(elem, null, true);
          else obj = elem;

          resultPojo.add(obj);
        } else {
          resultPojo.add(doc);
        }
      }

      return (RET) resultPojo;
    } finally {
      result.close();
    }
  }

  public <RET extends List<?>> RET objectCommand(String iCommand, Object... iArgs) {
    checkOpenness();

    convertParameters(iArgs);

    final OResultSet result = underlying.command(iCommand, iArgs);

    if (result == null) return null;

    try {

      final List<Object> resultPojo = new ArrayList<Object>();
      Object obj;
      while (result.hasNext()) {
        OResult doc = result.next();
        if (doc.isElement()) {
          // GET THE ASSOCIATED DOCUMENT
          OElement elem = doc.getElement().get();
          if (elem.getSchemaType().isPresent()) obj = getUserObjectByRecord(elem, null, true);
          else obj = elem;

          resultPojo.add(obj);
        } else {
          resultPojo.add(doc);
        }
      }

      return (RET) resultPojo;
    } finally {
      result.close();
    }
  }

  public <RET extends List<?>> RET objectCommand(String iCommand, Map<String, Object> iArgs) {
    checkOpenness();

    convertParameters(iArgs);

    final OResultSet result = underlying.command(iCommand, iArgs);

    if (result == null) return null;

    try {

      final List<Object> resultPojo = new ArrayList<Object>();
      Object obj;
      while (result.hasNext()) {
        OResult doc = result.next();
        if (doc.isElement()) {
          // GET THE ASSOCIATED DOCUMENT
          OElement elem = doc.getElement().get();
          if (elem.getSchemaType().isPresent()) obj = getUserObjectByRecord(elem, null, true);
          else obj = elem;

          resultPojo.add(obj);
        } else {
          resultPojo.add(doc);
        }
      }

      return (RET) resultPojo;
    } finally {
      result.close();
    }
  }

  @Override
  public OResultSet command(String query, Map args)
      throws OCommandSQLParsingException, OCommandExecutionException {
    return underlying.command(query, args);
  }

  @Override
  public OResultSet execute(String language, String script, Object... args)
      throws OCommandExecutionException, OCommandScriptException {
    return underlying.execute(language, script, args);
  }

  @Override
  public OResultSet execute(String language, String script, Map<String, ?> args)
      throws OCommandExecutionException, OCommandScriptException {
    return underlying.execute(language, script, args);
  }

  @Override
  public <T> T executeWithRetry(int nRetries, Function<ODatabaseSession, T> function)
      throws IllegalStateException, IllegalArgumentException, ONeedRetryException,
          UnsupportedOperationException {
    return underlying.executeWithRetry(nRetries, function);
  }

  @Override
  public OStorageInfo getStorageInfo() {
    return getStorage();
  }
}
