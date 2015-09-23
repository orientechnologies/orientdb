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
package com.orientechnologies.orient.object.db;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.entity.OEntityManager;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.metadata.security.OToken;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.OSerializationThreadLocal;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.tx.OTransactionNoTx;
import com.orientechnologies.orient.core.version.ORecordVersion;
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
import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Object Database instance. It's a wrapper to the class ODatabaseDocumentTx that handles conversion between ODocument instances and
 * POJOs using javassist APIs.
 * 
 * @see ODatabaseDocumentTx
 * @author Luca Molino
 */
@SuppressWarnings("unchecked")
public class OObjectDatabaseTx extends ODatabasePojoAbstract<Object> implements ODatabaseObject, ODatabaseInternal<Object>,
    OUserObject2RecordHandler {

  public static final String    TYPE = "object";
  protected ODictionary<Object> dictionary;
  protected OEntityManager      entityManager;
  protected boolean             saveOnlyDirty;
  protected boolean             lazyLoading;
  protected boolean             automaticSchemaGeneration;
  protected OMetadataObject     metadata;

  public OObjectDatabaseTx(final String iURL) {
    super(new ODatabaseDocumentTx(iURL));
    underlying.setDatabaseOwner(this);
    init();
  }

  /**
   * Constructor to wrap an existing database connect for object connections
   * 
   * @param iDatabase
   *          an open database connection
   */
  public OObjectDatabaseTx(ODatabaseDocumentTx iDatabase) {
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
    entityManager.registerEntityClass(OUser.class);
    entityManager.registerEntityClass(ORole.class);
    metadata = new OMetadataObject((OMetadataInternal) underlying.getMetadata());
    return (THISDB) this;
  }

  @Override
  public <THISDB extends ODatabase> THISDB open(OToken iToken) {
    super.open(iToken);
    entityManager.registerEntityClass(OUser.class);
    entityManager.registerEntityClass(ORole.class);
    metadata = new OMetadataObject((OMetadataInternal) underlying.getMetadata());
    return (THISDB) this;
  }

  @Override
  public OMetadataObject getMetadata() {
    checkOpeness();
    if (metadata == null)
      metadata = new OMetadataObject((OMetadataInternal) underlying.getMetadata());
    return metadata;
  }

  @Override
  public Iterable<ODatabaseListener> getListeners() {
    return underlying.getListeners();
  }

  /**
   * Create a new POJO by its class name. Assure to have called the registerEntityClasses() declaring the packages that are part of
   * entity classes.
   * 
   * @see OEntityManager#registerEntityClasses(String)
   */
  public <RET extends Object> RET newInstance(final String iClassName, final Object iEnclosingClass, Object... iArgs) {
    underlying.checkIfActive();

    checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_CREATE, iClassName);

    try {
      Class<?> entityClass = entityManager.getEntityClass(iClassName);
      if (entityClass != null) {
        RET enhanced = (RET) OObjectEntityEnhancer.getInstance().getProxiedInstance(entityManager.getEntityClass(iClassName),
            iEnclosingClass, underlying.newInstance(iClassName), null, iArgs);
        return (RET) enhanced;
      } else {
        throw new OSerializationException("Type " + iClassName
            + " cannot be serialized because is not part of registered entities. To fix this error register this class");
      }
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on creating object of class " + iClassName, e, ODatabaseException.class);
    }
    return null;
  }

  /**
   * Create a new POJO by its class name. Assure to have called the registerEntityClasses() declaring the packages that are part of
   * entity classes.
   * 
   * @see OEntityManager#registerEntityClasses(String)
   */
  public <RET extends Object> RET newInstance(final String iClassName, final Object iEnclosingClass, ODocument iDocument,
      Object... iArgs) {
    checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_CREATE, iClassName);

    try {
      Class<?> entityClass = entityManager.getEntityClass(iClassName);
      if (entityClass != null) {
        RET enhanced = (RET) OObjectEntityEnhancer.getInstance().getProxiedInstance(entityManager.getEntityClass(iClassName),
            iEnclosingClass, iDocument, null, iArgs);
        return (RET) enhanced;
      } else {
        throw new OSerializationException("Type " + iClassName
            + " cannot be serialized because is not part of registered entities. To fix this error register this class");
      }
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on creating object of class " + iClassName, e, ODatabaseException.class);
    }
    return null;
  }

  public <RET> OObjectIteratorClass<RET> browseClass(final Class<RET> iClusterClass) {
    return browseClass(iClusterClass, true);
  }

  public <RET> OObjectIteratorClass<RET> browseClass(final Class<RET> iClusterClass, final boolean iPolymorphic) {
    if (iClusterClass == null)
      return null;

    return browseClass(iClusterClass.getSimpleName(), iPolymorphic);
  }

  public <RET> OObjectIteratorClass<RET> browseClass(final String iClassName) {
    return browseClass(iClassName, true);
  }

  public <RET> OObjectIteratorClass<RET> browseClass(final String iClassName, final boolean iPolymorphic) {
    checkOpeness();
    checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_READ, iClassName);

    return new OObjectIteratorClass<RET>(this, (ODatabaseDocumentTx) getUnderlying(), iClassName, iPolymorphic);
  }

  public <RET> OObjectIteratorCluster<RET> browseCluster(final String iClusterName) {
    checkOpeness();
    checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_READ, iClusterName);

    return (OObjectIteratorCluster<RET>) new OObjectIteratorCluster<Object>(this, (ODatabaseDocumentTx) getUnderlying(),
        getClusterIdByName(iClusterName));
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
    checkOpeness();
    if (iObject == null)
      return null;

    // GET THE ASSOCIATED DOCUMENT
    final ODocument record = getRecordByUserObject(iObject, true);
    underlying.reload(record, iFetchPlan, iIgnoreCache, force);

    iObject = stream2pojo(record, iObject, iFetchPlan, true);
    return (RET) iObject;

  }

  public <RET> RET load(final Object iPojo, final String iFetchPlan) {
    return (RET) load(iPojo, iFetchPlan, false);
  }

  @Override
  public void attach(final Object iPojo) {
    OObjectEntitySerializer.attach(iPojo, this);
  }

  public <RET> RET attachAndSave(final Object iPojo) {
    attach(iPojo);
    return (RET) save(iPojo);
  }

  @Override
  /**
   * Method that detaches all fields contained in the document to the given object. It returns by default a proxied instance. To get
   * a detached non proxied instance @see {@link OObjectEntitySerializer.detach(T o, ODatabaseObject db, boolean
   * returnNonProxiedInstance)}
   * 
   * @param <T>
   * @param o
   *          :- the object to detach
   * @return the detached object
   */
  public <RET> RET detach(final Object iPojo) {
    return (RET) OObjectEntitySerializer.detach(iPojo, this);
  }

  /**
   * Method that detaches all fields contained in the document to the given object.
   * 
   * @param <RET>
   * @param iPojo
   *          :- the object to detach
   * @param returnNonProxiedInstance
   *          :- defines if the return object will be a proxied instance or not. If set to TRUE and the object does not contains @Id
   *          and @Version fields it could procude data replication
   * @return the object serialized or with detached data
   */
  public <RET> RET detach(final Object iPojo, boolean returnNonProxiedInstance) {
    return (RET) OObjectEntitySerializer.detach(iPojo, this, returnNonProxiedInstance);
  }

  /**
   * Method that detaches all fields contained in the document to the given object and recursively all object tree. This may throw a
   * {@link StackOverflowError} with big objects tree. To avoid it set the stack size with -Xss java option
   * 
   * @param <RET>
   * @param iPojo
   *          :- the object to detach
   * @param returnNonProxiedInstance
   *          :- defines if the return object will be a proxied instance or not. If set to TRUE and the object does not contains @Id
   *          and @Version fields it could procude data replication
   * @return the object serialized or with detached data
   */
  public <RET> RET detachAll(final Object iPojo, boolean returnNonProxiedInstance) {
    return detachAll(iPojo, returnNonProxiedInstance, new HashMap<Object, Object>());
  }

  public <RET> RET load(final Object iPojo, final String iFetchPlan, final boolean iIgnoreCache) {
    return (RET) load(iPojo, iFetchPlan, iIgnoreCache, false, OStorage.LOCKING_STRATEGY.DEFAULT);
  }

  @Override
  @Deprecated
  public <RET> RET load(Object iPojo, String iFetchPlan, boolean iIgnoreCache, boolean loadTombstone,
      OStorage.LOCKING_STRATEGY iLockingStrategy) {
    return load(iPojo, iFetchPlan, iIgnoreCache, !iIgnoreCache, loadTombstone, iLockingStrategy);
  }

  @Override
  @Deprecated
  public <RET> RET load(Object iPojo, String iFetchPlan, boolean iIgnoreCache, final boolean iUpdateCache, boolean loadTombstone,
      OStorage.LOCKING_STRATEGY iLockingStrategy) {
    checkOpeness();
    if (iPojo == null)
      return null;

    // GET THE ASSOCIATED DOCUMENT
    ODocument record = getRecordByUserObject(iPojo, true);
    try {
      record.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);

      record = underlying.load(record, iFetchPlan, iIgnoreCache, iUpdateCache, loadTombstone, OStorage.LOCKING_STRATEGY.DEFAULT);

      return (RET) stream2pojo(record, iPojo, iFetchPlan);
    } finally {
      record.setInternalStatus(ORecordElement.STATUS.LOADED);
    }
  }

  public <RET> RET load(final ORID recordId) {
    return (RET) load(recordId, null);
  }

  public <RET> RET load(final ORID iRecordId, final String iFetchPlan) {
    return (RET) load(iRecordId, iFetchPlan, false);
  }

  public <RET> RET load(final ORID iRecordId, final String iFetchPlan, final boolean iIgnoreCache) {
    return (RET) load(iRecordId, iFetchPlan, iIgnoreCache, !iIgnoreCache, false, OStorage.LOCKING_STRATEGY.DEFAULT);
  }

  @Override
  @Deprecated
  public <RET> RET load(ORID iRecordId, String iFetchPlan, boolean iIgnoreCache, boolean loadTombstone,
      OStorage.LOCKING_STRATEGY iLockingStrategy) {
    return load(iRecordId, iFetchPlan, iIgnoreCache, !iIgnoreCache, loadTombstone, iLockingStrategy);
  }

  @Override
  @Deprecated
  public <RET> RET load(ORID iRecordId, String iFetchPlan, boolean iIgnoreCache, final boolean iUpdateCache, boolean loadTombstone,
      OStorage.LOCKING_STRATEGY iLockingStrategy) {
    checkOpeness();
    if (iRecordId == null)
      return null;

    // GET THE ASSOCIATED DOCUMENT
    final ODocument record = (ODocument) underlying.load(iRecordId, iFetchPlan, iIgnoreCache, iUpdateCache, loadTombstone,
        OStorage.LOCKING_STRATEGY.DEFAULT);
    if (record == null)
      return null;

    return (RET) OObjectEntityEnhancer.getInstance().getProxiedInstance(record.getClassName(), entityManager, record, null);
  }

  /**
   * Saves an object to the databasein synchronous mode . First checks if the object is new or not. In case it's new a new ODocument
   * is created and bound to the object, otherwise the ODocument is retrieved and updated. The object is introspected using the Java
   * Reflection to extract the field values. <br>
   * If a multi value (array, collection or map of objects) is passed, then each single object is stored separately.
   */
  public <RET> RET save(final Object iContent) {
    return (RET) save(iContent, (String) null, OPERATION_MODE.SYNCHRONOUS, false, null, null);
  }

  /**
   * Saves an object to the database specifying the mode. First checks if the object is new or not. In case it's new a new ODocument
   * is created and bound to the object, otherwise the ODocument is retrieved and updated. The object is introspected using the Java
   * Reflection to extract the field values. <br>
   * If a multi value (array, collection or map of objects) is passed, then each single object is stored separately.
   */
  public <RET> RET save(final Object iContent, OPERATION_MODE iMode, boolean iForceCreate,
      final ORecordCallback<? extends Number> iRecordCreatedCallback, ORecordCallback<ORecordVersion> iRecordUpdatedCallback) {
    return (RET) save(iContent, null, iMode, false, iRecordCreatedCallback, iRecordUpdatedCallback);
  }

  /**
   * Saves an object in synchronous mode to the database forcing a record cluster where to store it. First checks if the object is
   * new or not. In case it's new a new ODocument is created and bound to the object, otherwise the ODocument is retrieved and
   * updated. The object is introspected using the Java Reflection to extract the field values. <br>
   * If a multi value (array, collection or map of objects) is passed, then each single object is stored separately.
   * 
   * Before to use the specified cluster a check is made to know if is allowed and figures in the configured and the record is valid
   * following the constraints declared in the schema.
   * 
   * @see ODocument#validate()
   */
  public <RET> RET save(final Object iPojo, final String iClusterName) {
    return (RET) save(iPojo, iClusterName, OPERATION_MODE.SYNCHRONOUS, false, null, null);
  }

  /**
   * Saves an object to the database forcing a record cluster where to store it. First checks if the object is new or not. In case
   * it's new a new ODocument is created and bound to the object, otherwise the ODocument is retrieved and updated. The object is
   * introspected using the Java Reflection to extract the field values. <br>
   * If a multi value (array, collection or map of objects) is passed, then each single object is stored separately.
   * 
   * Before to use the specified cluster a check is made to know if is allowed and figures in the configured and the record is valid
   * following the constraints declared in the schema.
   * 
   * @see ODocument#validate()
   */
  public <RET> RET save(final Object iPojo, final String iClusterName, OPERATION_MODE iMode, boolean iForceCreate,
      final ORecordCallback<? extends Number> iRecordCreatedCallback, ORecordCallback<ORecordVersion> iRecordUpdatedCallback) {
    checkOpeness();
    if (iPojo == null)
      return (RET) iPojo;
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

          ODocument savedRecord = underlying.save(record, iClusterName, iMode, iForceCreate, iRecordCreatedCallback,
              iRecordUpdatedCallback);

          ((OObjectProxyMethodHandler) ((ProxyObject) proxiedObject).getHandler()).setDoc(savedRecord);
          ((OObjectProxyMethodHandler) ((ProxyObject) proxiedObject).getHandler()).updateLoadedFieldMap(proxiedObject, false);
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
    checkOpeness();

    if (iPojo == null)
      return this;

    ODocument record = getRecordByUserObject(iPojo, false);
    if (record == null) {
      final ORecordId rid = OObjectSerializerHelper.getObjectID(this, iPojo);
      if (rid == null)
        throw new OObjectNotDetachedException("Cannot retrieve the object's ID for '" + iPojo + "' because has not been detached");

      record = (ODocument) underlying.load(rid);
    }
    deleteCascade(record);

    underlying.delete(record);

    if (getTransaction() instanceof OTransactionNoTx)
      unregisterPojo(iPojo, record);

    return this;
  }

  @Override
  public ODatabaseObject delete(final ORID iRID) {
    checkOpeness();

    if (iRID == null)
      return this;

    final ORecord record = iRID.getRecord();
    if (record instanceof ODocument) {
      Object iPojo = getUserObjectByRecord(record, null);

      deleteCascade((ODocument) record);

      underlying.delete(record);

      if (getTransaction() instanceof OTransactionNoTx)
        unregisterPojo(iPojo, (ODocument) record);

    }
    return this;
  }

  @Override
  public ODatabaseObject delete(final ORID iRID, final ORecordVersion iVersion) {
    deleteRecord(iRID, iVersion, false);
    return this;
  }

  @Override
  public boolean hide(ORID rid) {
    throw new UnsupportedOperationException("hide");
  }

  @Override
  public ODatabase<Object> cleanOutRecord(ORID iRID, ORecordVersion iVersion) {
    deleteRecord(iRID, iVersion, true);
    return this;
  }

  public long countClass(final String iClassName) {
    checkOpeness();
    return underlying.countClass(iClassName);
  }

  public long countClass(final String iClassName, final boolean iPolymorphic) {
    checkOpeness();
    return underlying.countClass(iClassName, iPolymorphic);
  }

  public long countClass(final Class<?> iClass) {
    checkOpeness();
    return underlying.countClass(iClass.getSimpleName());
  }

  public ODictionary<Object> getDictionary() {
    checkOpeness();
    if (dictionary == null)
      dictionary = new ODictionaryWrapper(this, underlying.getDictionary().getIndex());

    return dictionary;
  }

  @Override
  public ODatabasePojoAbstract<Object> commit() {
    // BY PASS DOCUMENT DB
    return (ODatabasePojoAbstract<Object>) commit(false);
  }

  @Override
  public ODatabasePojoAbstract<Object> commit(boolean force) throws OTransactionException {
    underlying.commit(force);

    if (getTransaction().isActive())
      return this;

    if (getTransaction().getAllRecordEntries() != null) {
      // UPDATE ID & VERSION FOR ALL THE RECORDS
      Object pojo = null;
      for (ORecordOperation entry : getTransaction().getAllRecordEntries()) {
        switch (entry.type) {
        case ORecordOperation.CREATED:
        case ORecordOperation.UPDATED:
          break;

        case ORecordOperation.DELETED:
          final ORecord rec = entry.getRecord();
          if (rec instanceof ODocument)
            unregisterPojo(pojo, (ODocument) rec);
          break;
        }
      }
    }

    return this;
  }

  @Override
  public ODatabasePojoAbstract<Object> rollback() {
    return rollback(false);
  }

  @Override
  public ODatabasePojoAbstract<Object> rollback(boolean force) throws OTransactionException {
    // BY PASS DOCUMENT DB
    underlying.rollback(force);

    if (!underlying.getTransaction().isActive()) {
      // COPY ALL TX ENTRIES
      final List<ORecordOperation> newEntries;
      if (getTransaction().getCurrentRecordEntries() != null) {
        newEntries = new ArrayList<ORecordOperation>();
        for (ORecordOperation entry : getTransaction().getCurrentRecordEntries())
          if (entry.type == ORecordOperation.CREATED)
            newEntries.add(entry);
      } else
        newEntries = null;
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
   * @param iPojo
   *          User object
   */
  @Override
  public ORecordVersion getVersion(final Object iPojo) {
    checkOpeness();
    final ODocument record = getRecordByUserObject(iPojo, false);
    if (record != null)
      return record.getRecordVersion();

    return OObjectSerializerHelper.getObjectVersion(iPojo);
  }

  /**
   * Returns the object unique identity.
   * 
   * @param iPojo
   *          User object
   */
  @Override
  public ORID getIdentity(final Object iPojo) {
    checkOpeness();
    if (iPojo instanceof OIdentifiable)
      return ((OIdentifiable) iPojo).getIdentity();
    final ODocument record = getRecordByUserObject(iPojo, false);
    if (record != null)
      return record.getIdentity();
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
    checkOpeness();
    return new ODocument();
  }

  public <DBTYPE extends ODatabase> DBTYPE checkSecurity(ORule.ResourceGeneric resourceGeneric, String resourceSpecific,
      final byte iOperation) {
    return (DBTYPE) underlying.checkSecurity(resourceGeneric, resourceSpecific, iOperation);
  }

  public <DBTYPE extends ODatabase> DBTYPE checkSecurity(final ORule.ResourceGeneric iResource, final int iOperation,
      Object iResourceSpecific) {
    return (DBTYPE) underlying.checkSecurity(iResource, iOperation, iResourceSpecific);
  }

  public <DBTYPE extends ODatabase> DBTYPE checkSecurity(final ORule.ResourceGeneric iResource, final int iOperation,
      Object... iResourcesSpecific) {
    return (DBTYPE) underlying.checkSecurity(iResource, iOperation, iResourcesSpecific);
  }

  @Override
  public ODocument pojo2Stream(final Object iPojo, final ODocument iRecord) {
    if (iPojo instanceof ProxyObject) {
      return ((OObjectProxyMethodHandler) ((ProxyObject) iPojo).getHandler()).getDoc();
    }
    return OObjectSerializerHelper.toStream(iPojo, iRecord, getEntityManager(),
        getMetadata().getSchema().getClass(iPojo.getClass().getSimpleName()), this, this, saveOnlyDirty);
  }

  @Override
  public Object stream2pojo(ODocument iRecord, final Object iPojo, final String iFetchPlan) {
    return stream2pojo(iRecord, iPojo, iFetchPlan, false);
  }

  public Object stream2pojo(ODocument iRecord, final Object iPojo, final String iFetchPlan, boolean iReload) {
    if (iRecord.getInternalStatus() == ORecordElement.STATUS.NOT_LOADED)
      iRecord = (ODocument) iRecord.load();
    if (iReload) {
      if (iPojo != null) {
        if (iPojo instanceof Proxy) {
          ((OObjectProxyMethodHandler) ((ProxyObject) iPojo).getHandler()).setDoc(iRecord);
          ((OObjectProxyMethodHandler) ((ProxyObject) iPojo).getHandler()).updateLoadedFieldMap(iPojo, iReload);
          return iPojo;
        } else
          return OObjectEntityEnhancer.getInstance().getProxiedInstance(iPojo.getClass(), iRecord);
      } else
        return OObjectEntityEnhancer.getInstance().getProxiedInstance(iRecord.getClassName(), entityManager, iRecord, null);
    } else if (!(iPojo instanceof Proxy))
      return OObjectEntityEnhancer.getInstance().getProxiedInstance(iPojo.getClass(), iRecord);
    else
      return iPojo;
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
    getStorage().setConflictStrategy(Orient.instance().getRecordConflictStrategy().getStrategy(iStrategyName));
    return this;
  }

  @Override
  public ODocument getRecordByUserObject(final Object iPojo, final boolean iCreateIfNotAvailable) {
    if (iPojo instanceof Proxy)
      return OObjectEntitySerializer.getDocument((Proxy) iPojo);
    return OObjectEntitySerializer.getDocument((Proxy) OObjectEntitySerializer.serializeObject(iPojo, this));
  }

  @Override
  public Object getUserObjectByRecord(final OIdentifiable iRecord, final String iFetchPlan, final boolean iCreate) {
    final ODocument document = iRecord.getRecord();

    return OObjectEntityEnhancer.getInstance().getProxiedInstance(document.getClassName(), getEntityManager(), document, null);
  }

  @Override
  public void registerUserObject(final Object iObject, final ORecord iRecord) {
  }

  public void registerUserObjectAfterLinkSave(ORecord iRecord) {
  }

  @Override
  public void unregisterPojo(final Object iObject, final ODocument iRecord) {
  }

  public void registerClassMethodFilter(Class<?> iClass, OObjectMethodFilter iMethodFilter) {
    OObjectEntityEnhancer.getInstance().registerClassMethodFilter(iClass, iMethodFilter);
  }

  public void deregisterClassMethodFilter(final Class<?> iClass) {
    OObjectEntityEnhancer.getInstance().deregisterClassMethodFilter(iClass);
  }

  @Override
  public void resetInitialization() {
    underlying.resetInitialization();
  }

  protected <RET> RET detachAll(final Object iPojo, boolean returnNonProxiedInstance, Map<Object, Object> alreadyDetached) {
    return (RET) OObjectEntitySerializer.detachAll(iPojo, this, returnNonProxiedInstance, alreadyDetached);
  }

  protected void deleteCascade(final ODocument record) {
    if (record == null)
      return;
    List<String> toDeleteCascade = OObjectEntitySerializer.getCascadeDeleteFields(record.getClassName());
    if (toDeleteCascade != null) {
      for (String field : toDeleteCascade) {
        Object toDelete = record.field(field);
        if (toDelete instanceof OIdentifiable) {
          if (toDelete != null)
            delete(((OIdentifiable) toDelete).getIdentity());
        } else if (toDelete instanceof Collection) {
          for (OIdentifiable cascadeRecord : ((Collection<OIdentifiable>) toDelete)) {
            if (cascadeRecord != null)
              delete(((OIdentifiable) cascadeRecord).getIdentity());
          }
        } else if (toDelete instanceof Map) {
          for (OIdentifiable cascadeRecord : ((Map<Object, OIdentifiable>) toDelete).values()) {
            if (cascadeRecord != null)
              delete(((OIdentifiable) cascadeRecord).getIdentity());
          }
        }
      }
    }
  }

  protected void init() {
    entityManager = OEntityManager.getEntityManagerByDatabaseURL(getURL());
    entityManager.setClassHandler(OObjectEntityClassHandler.getInstance());
    saveOnlyDirty = OGlobalConfiguration.OBJECT_SAVE_ONLY_DIRTY.getValueAsBoolean();
    OObjectSerializerHelper.register();
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
      underlying.delete(doc);
    }
    handler.getOrphans().clear();
  }

  private boolean deleteRecord(ORID iRID, ORecordVersion iVersion, boolean prohibitTombstones) {
    checkOpeness();

    if (iRID == null)
      return true;

    ODocument record = iRID.getRecord();
    if (record != null) {
      Object iPojo = getUserObjectByRecord(record, null);

      deleteCascade(record);

      if (prohibitTombstones)
        underlying.cleanOutRecord(iRID, iVersion);
      else
        underlying.delete(iRID, iVersion);

      if (getTransaction() instanceof OTransactionNoTx)
        unregisterPojo(iPojo, record);

    }
    return false;
  }
}
