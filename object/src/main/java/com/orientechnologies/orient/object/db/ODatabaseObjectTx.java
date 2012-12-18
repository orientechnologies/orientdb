/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.object.db;

import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.entity.OEntityManager;
import com.orientechnologies.orient.core.entity.OEntityManagerClassHandler;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.OSerializationThreadLocal;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.tx.OTransactionNoTx;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.object.dictionary.ODictionaryWrapper;
import com.orientechnologies.orient.object.iterator.OObjectIteratorClass;
import com.orientechnologies.orient.object.iterator.OObjectIteratorCluster;
import com.orientechnologies.orient.object.serialization.OObjectSerializerHelper;

/**
 * DEPRECATED -- USE {@link OObjectDatabaseTx} instead
 * 
 * Object Database instance. It's a wrapper to the class ODatabaseDocumentTx but handle the conversion between ODocument instances
 * and POJOs.
 * 
 * @see OObjectDatabaseTx
 * @author Luca Garulli
 */
@Deprecated
@SuppressWarnings("unchecked")
public class ODatabaseObjectTx extends ODatabasePojoAbstract<Object> implements ODatabaseObject, OUserObject2RecordHandler {

  public static final String    TYPE = "object";
  protected ODictionary<Object> dictionary;
  protected OEntityManager      entityManager;
  protected boolean             saveOnlyDirty;
  protected boolean             lazyLoading;

  public ODatabaseObjectTx(final String iURL) {
    super(new ODatabaseDocumentTx(iURL));
    underlying.setDatabaseOwner(this);
    init();
  }

  public <T> T newInstance(final Class<T> iType) {
    return (T) newInstance(iType.getName());
  }

  /**
   * Create a new POJO by its class name. Assure to have called the registerEntityClasses() declaring the packages that are part of
   * entity classes.
   * 
   * @see OEntityManager#registerEntityClasses(String)
   */
  public <RET extends Object> RET newInstance(final String iClassName) {
    checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_CREATE, iClassName);

    try {
      return (RET) entityManager.createPojo(iClassName);
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
    checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_READ, iClassName);

    return new OObjectIteratorClass<RET>(this, (ODatabaseRecordAbstract) getUnderlying().getUnderlying(), iClassName, iPolymorphic);
  }

  public <RET> OObjectIteratorCluster<RET> browseCluster(final String iClusterName) {
    checkOpeness();
    checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, iClusterName);

    return (OObjectIteratorCluster<RET>) new OObjectIteratorCluster<Object>(this, (ODatabaseRecordAbstract) getUnderlying()
        .getUnderlying(), getClusterIdByName(iClusterName));
  }

  public Object load(final Object iPojo) {
    return load(iPojo, null);
  }

  public Object reload(final Object iPojo) {
    return reload(iPojo, null, true);
  }

  public Object reload(final Object iPojo, final String iFetchPlan, final boolean iIgnoreCache) {
    checkOpeness();
    if (iPojo == null)
      return iPojo;

    // GET THE ASSOCIATED DOCUMENT
    final ODocument record = getRecordByUserObject(iPojo, true);
    underlying.reload(record, iFetchPlan, iIgnoreCache);

    stream2pojo(record, iPojo, iFetchPlan);
    return iPojo;
  }

  public Object load(final Object iPojo, final String iFetchPlan) {
    return load(iPojo, iFetchPlan, false);
  }

  public Object load(final Object iPojo, final String iFetchPlan, final boolean iIgnoreCache) {
    return load(iPojo, iFetchPlan, iIgnoreCache, false);
  }

  @Override
  public Object load(Object iPojo, String iFetchPlan, boolean iIgnoreCache, boolean loadTombstone) {
    checkOpeness();
    if (iPojo == null)
      return this;

    // GET THE ASSOCIATED DOCUMENT
    ODocument record = getRecordByUserObject(iPojo, true);
    try {
      record.setInternalStatus(com.orientechnologies.orient.core.db.record.ORecordElement.STATUS.UNMARSHALLING);

      record = underlying.load(record, iFetchPlan, iIgnoreCache, loadTombstone);

      stream2pojo(record, iPojo, iFetchPlan);
    } finally {
      record.setInternalStatus(com.orientechnologies.orient.core.db.record.ORecordElement.STATUS.LOADED);
    }

    return this;
  }

  public Object load(final ORID iRecordId) {
    return load(iRecordId, null);
  }

  public Object load(final ORID iRecordId, final String iFetchPlan) {
    return load(iRecordId, iFetchPlan, false);
  }

  public Object load(final ORID iRecordId, final String iFetchPlan, final boolean iIgnoreCache) {
    return load(iRecordId, iFetchPlan, iIgnoreCache, false);
  }

  @Override
  public Object load(ORID iRecordId, String iFetchPlan, boolean iIgnoreCache, boolean loadTombstone) {
    checkOpeness();
    if (iRecordId == null)
      return null;

    ODocument record = rid2Records.get(iRecordId);
    if (record == null) {
      // GET THE ASSOCIATED DOCUMENT
      record = (ODocument) underlying.load(iRecordId, iFetchPlan, iIgnoreCache, loadTombstone);
      if (record == null)
        return null;
    }

    Object result = records2Objects.get(record);
    if (result != null)
      // FOUND: JUST RETURN IT
      return result;

    result = stream2pojo(record, newInstance(record.getClassName()), iFetchPlan);
    registerUserObject(result, record);
    return result;
  }

  /**
   * Saves an object to the databasein synchronous mode . First checks if the object is new or not. In case it's new a new ODocument
   * is created and bound to the object, otherwise the ODocument is retrieved and updated. The object is introspected using the Java
   * Reflection to extract the field values. <br/>
   * If a multi value (array, collection or map of objects) is passed, then each single object is stored separately.
   */
  public Object save(final Object iContent) {
    return save(iContent, (String) null, OPERATION_MODE.SYNCHRONOUS, false, null, null);
  }

  /**
   * Saves an object to the database specifying the mode. First checks if the object is new or not. In case it's new a new ODocument
   * is created and bound to the object, otherwise the ODocument is retrieved and updated. The object is introspected using the Java
   * Reflection to extract the field values. <br/>
   * If a multi value (array, collection or map of objects) is passed, then each single object is stored separately.
   */
  public <RET> RET save(final Object iContent, OPERATION_MODE iMode, boolean iForceCreate,
      final ORecordCallback<? extends Number> iRecordCreatedCallback, ORecordCallback<ORecordVersion> iRecordUpdatedCallback) {
    return (RET) save(iContent, null, iMode, iForceCreate, iRecordCreatedCallback, iRecordUpdatedCallback);
  }

  /**
   * Saves an object in synchronous mode to the database forcing a record cluster where to store it. First checks if the object is
   * new or not. In case it's new a new ODocument is created and bound to the object, otherwise the ODocument is retrieved and
   * updated. The object is introspected using the Java Reflection to extract the field values. <br/>
   * If a multi value (array, collection or map of objects) is passed, then each single object is stored separately.
   * 
   * Before to use the specified cluster a check is made to know if is allowed and figures in the configured and the record is valid
   * following the constraints declared in the schema.
   * 
   * @see ORecordSchemaAware#validate()
   */
  public Object save(final Object iPojo, final String iClusterName) {
    return save(iPojo, iClusterName, OPERATION_MODE.SYNCHRONOUS, false, null, null);
  }

  /**
   * Saves an object to the database forcing a record cluster where to store it. First checks if the object is new or not. In case
   * it's new a new ODocument is created and bound to the object, otherwise the ODocument is retrieved and updated. The object is
   * introspected using the Java Reflection to extract the field values. <br/>
   * If a multi value (array, collection or map of objects) is passed, then each single object is stored separately.
   * 
   * Before to use the specified cluster a check is made to know if is allowed and figures in the configured and the record is valid
   * following the constraints declared in the schema.
   * 
   * @see ORecordSchemaAware#validate()
   */
  public <RET> RET save(final Object iPojo, final String iClusterName, OPERATION_MODE iMode, boolean iForceCreate,
      final ORecordCallback<? extends Number> iRecordCreatedCallback, ORecordCallback<ORecordVersion> iRecordUpdatedCallback) {
    checkOpeness();

    if (iPojo == null)
      return null;
    else if (OMultiValue.isMultiValue(iPojo)) {
      // MULTI VALUE OBJECT: STORE SINGLE POJOS
      for (Object pojo : OMultiValue.getMultiValueIterable(iPojo)) {
        save(pojo, iClusterName);
      }
    } else {
      OSerializationThreadLocal.INSTANCE.get().clear();

      // GET THE ASSOCIATED DOCUMENT
      final ODocument record = getRecordByUserObject(iPojo, true);
      try {
        record.setInternalStatus(com.orientechnologies.orient.core.db.record.ORecordElement.STATUS.MARSHALLING);

        if (!saveOnlyDirty || record.isDirty()) {
          // REGISTER BEFORE TO SERIALIZE TO AVOID PROBLEMS WITH CIRCULAR DEPENDENCY
          // registerUserObject(iPojo, record);

          pojo2Stream(iPojo, record);

          underlying.save(record, iClusterName, iMode, iForceCreate, iRecordCreatedCallback, iRecordUpdatedCallback);

          // RE-REGISTER FOR NEW RECORDS SINCE THE ID HAS CHANGED
          registerUserObject(iPojo, record);
        }
      } finally {
        record.setInternalStatus(com.orientechnologies.orient.core.db.record.ORecordElement.STATUS.LOADED);
      }
    }
    return (RET) iPojo;
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

    underlying.delete(record);

    if (getTransaction() instanceof OTransactionNoTx)
      unregisterPojo(iPojo, record);

    return this;
  }

  public long countClass(final String iClassName) {
    checkOpeness();
    return underlying.countClass(iClassName);
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
    try {
      // BY PASS DOCUMENT DB
      ((ODatabaseRecordTx) underlying.getUnderlying()).commit();

      if (getTransaction().getAllRecordEntries() != null) {
        // UPDATE ID & VERSION FOR ALL THE RECORDS
        Object pojo = null;
        for (ORecordOperation entry : getTransaction().getAllRecordEntries()) {
          pojo = records2Objects.get(entry.getRecord());

          if (pojo != null)
            switch (entry.type) {
            case ORecordOperation.CREATED:
              rid2Records.put(entry.getRecord().getIdentity(), (ODocument) entry.getRecord());
              OObjectSerializerHelper.setObjectID(entry.getRecord().getIdentity(), pojo);

            case ORecordOperation.UPDATED:
              OObjectSerializerHelper.setObjectVersion(entry.getRecord().getRecordVersion().copy(), pojo);
              break;

            case ORecordOperation.DELETED:
              OObjectSerializerHelper.setObjectID(null, pojo);
              OObjectSerializerHelper.setObjectVersion(null, pojo);

              unregisterPojo(pojo, (ODocument) entry.getRecord());
              break;
            }
        }
      }
    } finally {
      getTransaction().close();
    }

    return this;
  }

  @Override
  public ODatabasePojoAbstract<Object> rollback() {
    try {
      // COPY ALL TX ENTRIES
      final List<ORecordOperation> newEntries;
      if (getTransaction().getCurrentRecordEntries() != null) {
        newEntries = new ArrayList<ORecordOperation>();
        for (ORecordOperation entry : getTransaction().getCurrentRecordEntries())
          if (entry.type == ORecordOperation.CREATED)
            newEntries.add(entry);
      } else
        newEntries = null;

      // BY PASS DOCUMENT DB
      ((ODatabaseRecordTx) underlying.getUnderlying()).rollback();

      if (newEntries != null) {
        Object pojo = null;
        for (ORecordOperation entry : newEntries) {
          pojo = records2Objects.get(entry.getRecord());

          OObjectSerializerHelper.setObjectID(null, pojo);
          OObjectSerializerHelper.setObjectVersion(null, pojo);
        }
      }

      if (getTransaction().getCurrentRecordEntries() != null)
        for (ORecordOperation recordEntry : getTransaction().getCurrentRecordEntries()) {
          rid2Records.remove(recordEntry.getRecord().getIdentity());
          final Object pojo = records2Objects.remove(recordEntry.getRecord());
          if (pojo != null)
            objects2Records.remove(pojo);
        }

      if (getTransaction().getAllRecordEntries() != null)
        for (ORecordOperation recordEntry : getTransaction().getAllRecordEntries()) {
          rid2Records.remove(recordEntry.getRecord().getIdentity());
          final Object pojo = records2Objects.remove(recordEntry.getRecord());
          if (pojo != null)
            objects2Records.remove(pojo);
        }

    } finally {
      getTransaction().close();
    }

    return this;
  }

  public OEntityManager getEntityManager() {
    return entityManager;
  }

  @Override
  public ODatabaseDocument getUnderlying() {
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

  public Object newInstance() {
    checkOpeness();
    return new ODocument();
  }

  public <DBTYPE extends ODatabase> DBTYPE checkSecurity(final String iResource, final byte iOperation) {
    return (DBTYPE) underlying.checkSecurity(iResource, iOperation);
  }

  public <DBTYPE extends ODatabase> DBTYPE checkSecurity(final String iResource, final int iOperation, Object iResourceSpecific) {
    return (DBTYPE) underlying.checkSecurity(iResource, iOperation, iResourceSpecific);
  }

  public <DBTYPE extends ODatabase> DBTYPE checkSecurity(final String iResource, final int iOperation, Object... iResourcesSpecific) {
    return (DBTYPE) underlying.checkSecurity(iResource, iOperation, iResourcesSpecific);
  }

  @Override
  public ODocument pojo2Stream(final Object iPojo, final ODocument iRecord) {
    return OObjectSerializerHelper.toStream(iPojo, iRecord, getEntityManager(),
        getMetadata().getSchema().getClass(iPojo.getClass().getSimpleName()), this, this, saveOnlyDirty);
  }

  @Override
  public Object stream2pojo(ODocument iRecord, final Object iPojo, final String iFetchPlan) {
    if (iRecord.getInternalStatus() == ORecordElement.STATUS.NOT_LOADED)
      iRecord = (ODocument) iRecord.load();

    return OObjectSerializerHelper.fromStream(iRecord, iPojo, getEntityManager(), this, iFetchPlan, lazyLoading);
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

  public void registerUserObjectAfterLinkSave(ORecordInternal<?> iRecord) {
    registerUserObject(getUserObjectByRecord(iRecord, null), iRecord);
  }

  protected void init() {
    entityManager = OEntityManager.getEntityManagerByDatabaseURL(getURL());
    entityManager.setClassHandler(new OEntityManagerClassHandler());
    saveOnlyDirty = OGlobalConfiguration.OBJECT_SAVE_ONLY_DIRTY.getValueAsBoolean();
    OObjectSerializerHelper.register();
    lazyLoading = true;
    if (!isClosed() && entityManager.getEntityClass(OUser.class.getSimpleName()) == null) {
      entityManager.registerEntityClass(OUser.class);
      entityManager.registerEntityClass(ORole.class);
    }
  }

}
