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
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyObject;

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
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.entity.OEntityManager;
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
import com.orientechnologies.orient.object.dictionary.ODictionaryWrapper;
import com.orientechnologies.orient.object.enhancement.OObjectEntityEnhancer;
import com.orientechnologies.orient.object.enhancement.OObjectEntitySerializer;
import com.orientechnologies.orient.object.enhancement.OObjectProxyMethodHandler;
import com.orientechnologies.orient.object.entity.OObjectEntityClassHandler;
import com.orientechnologies.orient.object.iterator.OObjectIteratorClass;
import com.orientechnologies.orient.object.iterator.OObjectIteratorCluster;
import com.orientechnologies.orient.object.serialization.OObjectSerializerHelper;

/**
 * Object Database instance. It's a wrapper to the class ODatabaseDocumentTx that handles conversion between ODocument instances and
 * POJOs using javassist APIs.
 * 
 * @see ODatabaseDocumentTx
 * @author Luca Molino
 */
@SuppressWarnings("unchecked")
public class OObjectDatabaseTx extends ODatabasePojoAbstract<Object> implements ODatabaseObject, OUserObject2RecordHandler {

	public static final String		TYPE	= "object";
	protected ODictionary<Object>	dictionary;
	protected OEntityManager			entityManager;
	protected boolean							saveOnlyDirty;
	protected boolean							lazyLoading;

	public OObjectDatabaseTx(final String iURL) {
		super(new ODatabaseDocumentTx(iURL));
		underlying.setDatabaseOwner(this);
		init();
	}

	public <T> T newInstance(final Class<T> iType) {
		return (T) newInstance(iType.getSimpleName(), null, new Object[0]);
	}

	public <T> T newInstance(final Class<T> iType, Object... iArgs) {
		return (T) newInstance(iType.getSimpleName(), null, iArgs);
	}

	public <RET> RET newInstance(String iClassName) {
		return (RET) newInstance(iClassName, null, new Object[0]);
	}

	@Override
	public <THISDB extends ODatabase> THISDB open(String iUserName, String iUserPassword) {
		super.open(iUserName, iUserPassword);
		entityManager.registerEntityClass(OUser.class);
		entityManager.registerEntityClass(ORole.class);
		return (THISDB) this;
	}

	/**
	 * Create a new POJO by its class name. Assure to have called the registerEntityClasses() declaring the packages that are part of
	 * entity classes.
	 * 
	 * @see OEntityManager.registerEntityClasses(String)
	 */
	public <RET extends Object> RET newInstance(final String iClassName, final Object iEnclosingClass, Object... iArgs) {
		checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_CREATE, iClassName);

		try {
			RET enhanced = (RET) OObjectEntityEnhancer.getInstance().getProxiedInstance(entityManager.getEntityClass(iClassName),
					iEnclosingClass, underlying.newInstance(iClassName), iArgs);
			return (RET) enhanced;
		} catch (Exception e) {
			OLogManager.instance().error(this, "Error on creating object of class " + iClassName, e, ODatabaseException.class);
		}
		return null;
	}

	/**
	 * Create a new POJO by its class name. Assure to have called the registerEntityClasses() declaring the packages that are part of
	 * entity classes.
	 * 
	 * @see OEntityManager.registerEntityClasses(String)
	 */
	public <RET extends Object> RET newInstance(final String iClassName, final Object iEnclosingClass, ODocument iDocument,
			Object... iArgs) {
		checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_CREATE, iClassName);

		try {
			RET enhanced = (RET) OObjectEntityEnhancer.getInstance().getProxiedInstance(entityManager.getEntityClass(iClassName),
					iEnclosingClass, iDocument, iArgs);
			return (RET) enhanced;
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

	public <RET> RET load(final Object iPojo) {
		return (RET) load(iPojo, null);
	}

	public <RET> RET reload(final Object iPojo) {
		return (RET) reload(iPojo, null, true);
	}

	public <RET> RET reload(Object iPojo, final String iFetchPlan, final boolean iIgnoreCache) {
		checkOpeness();
		if (iPojo == null)
			return null;

		// GET THE ASSOCIATED DOCUMENT
		final ODocument record = getRecordByUserObject(iPojo, true);
		underlying.reload(record, iFetchPlan, iIgnoreCache);

		iPojo = stream2pojo(record, iPojo, iFetchPlan, true);
		return (RET) iPojo;
	}

	public <RET> RET load(final Object iPojo, final String iFetchPlan) {
		return (RET) load(iPojo, iFetchPlan, false);
	}

	@Override
	public void attach(final Object iPojo) {
		OObjectEntitySerializer.attach(iPojo, this);
	}

	public void attachAndSave(final Object iPojo) {
		attach(iPojo);
		save(iPojo);
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
	 * @param <T>
	 * @param o
	 *          :- the object to detach
	 * @param db
	 *          :- the database instance
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
	 * @param <T>
	 * @param o
	 *          :- the object to detach
	 * @param returnNonProxiedInstance
	 *          :- defines if the return object will be a proxied instance or not. If set to TRUE and the object does not contains @Id
	 *          and @Version fields it could procude data replication
	 * @return the object serialized or with detached data
	 */
	public <RET> RET detachAll(final Object iPojo, boolean returnNonProxiedInstance) {
		return (RET) OObjectEntitySerializer.detachAll(iPojo, this, returnNonProxiedInstance);
	}

	public <RET> RET load(final Object iPojo, final String iFetchPlan, final boolean iIgnoreCache) {
		checkOpeness();
		if (iPojo == null)
			return null;

		// GET THE ASSOCIATED DOCUMENT
		ODocument record = getRecordByUserObject(iPojo, true);
		try {
			record.setInternalStatus(com.orientechnologies.orient.core.db.record.ORecordElement.STATUS.UNMARSHALLING);

			record = underlying.load(record, iFetchPlan, iIgnoreCache);

			return (RET) stream2pojo(record, iPojo, iFetchPlan);
		} finally {
			record.setInternalStatus(com.orientechnologies.orient.core.db.record.ORecordElement.STATUS.LOADED);
		}
	}

	public <RET> RET load(final ORID iRecordId) {
		return (RET) load(iRecordId, null);
	}

	public <RET> RET load(final ORID iRecordId, final String iFetchPlan) {
		return (RET) load(iRecordId, iFetchPlan, false);
	}

	public <RET> RET load(final ORID iRecordId, final String iFetchPlan, final boolean iIgnoreCache) {
		checkOpeness();
		if (iRecordId == null)
			return null;

		ODocument record = rid2Records.get(iRecordId);
		if (record == null) {
			// GET THE ASSOCIATED DOCUMENT
			record = (ODocument) underlying.load(iRecordId, iFetchPlan, iIgnoreCache);
			if (record == null)
				return null;
		}

		return (RET) OObjectEntityEnhancer.getInstance().getProxiedInstance(record.getClassName(), entityManager, record);
	}

	/**
	 * Saves an object to the databasein synchronous mode . First checks if the object is new or not. In case it's new a new ODocument
	 * is created and bound to the object, otherwise the ODocument is retrieved and updated. The object is introspected using the Java
	 * Reflection to extract the field values. <br/>
	 * If a multi value (array, collection or map of objects) is passed, then each single object is stored separately.
	 */
	public <RET> RET save(final Object iContent) {
		return (RET) save(iContent, (String) null, OPERATION_MODE.SYNCHRONOUS, null);
	}

	/**
	 * Saves an object to the database specifying the mode. First checks if the object is new or not. In case it's new a new ODocument
	 * is created and bound to the object, otherwise the ODocument is retrieved and updated. The object is introspected using the Java
	 * Reflection to extract the field values. <br/>
	 * If a multi value (array, collection or map of objects) is passed, then each single object is stored separately.
	 */
	public <RET> RET save(final Object iContent, OPERATION_MODE iMode, final ORecordCallback<? extends Number> iCallback) {
		return (RET) save(iContent, null, iMode, iCallback);
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
	public <RET> RET save(final Object iPojo, final String iClusterName) {
		return (RET) save(iPojo, iClusterName, OPERATION_MODE.SYNCHRONOUS, null);
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
	public <RET> RET save(final Object iPojo, final String iClusterName, OPERATION_MODE iMode,
			final ORecordCallback<? extends Number> iCallback) {
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
				record.setInternalStatus(com.orientechnologies.orient.core.db.record.ORecordElement.STATUS.MARSHALLING);

				if (!saveOnlyDirty || record.isDirty()) {
					// REGISTER BEFORE TO SERIALIZE TO AVOID PROBLEMS WITH CIRCULAR DEPENDENCY
					// registerUserObject(iPojo, record);

					underlying.save(record, iClusterName, iMode, iCallback);

					// RE-REGISTER FOR NEW RECORDS SINCE THE ID HAS CHANGED
					registerUserObject(proxiedObject, record);
				}
			} finally {
				record.setInternalStatus(com.orientechnologies.orient.core.db.record.ORecordElement.STATUS.LOADED);
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

		ODocument record = iRID.getRecord();
		Object iPojo = getUserObjectByRecord(record, null);

		deleteCascade(record);

		underlying.delete(record);

		if (getTransaction() instanceof OTransactionNoTx)
			unregisterPojo(iPojo, record);

		return this;
	}

	protected void deleteCascade(ODocument record) {
		List<String> toDeleteCascade = OObjectEntitySerializer.getCascadeDeleteFields(record.getClassName());
		if (toDeleteCascade != null) {
			for (String field : toDeleteCascade) {
				Object toDelete = record.field(field);
				if (toDelete instanceof OIdentifiable) {
					delete(((OIdentifiable) toDelete).getIdentity());
				} else if (toDelete instanceof Collection) {
					for (OIdentifiable cascadeRecord : ((Collection<OIdentifiable>) toDelete)) {
						delete(((OIdentifiable) cascadeRecord).getIdentity());
					}
				} else if (toDelete instanceof Map) {
					for (OIdentifiable cascadeRecord : ((Map<Object, OIdentifiable>) toDelete).values()) {
						delete(((OIdentifiable) cascadeRecord).getIdentity());
					}
				}
			}
		}
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
					switch (entry.type) {
					case ORecordOperation.CREATED:
						rid2Records.put(entry.getRecord().getIdentity(), (ODocument) entry.getRecord());

					case ORecordOperation.UPDATED:
						break;

					case ORecordOperation.DELETED:
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

			if (getTransaction().getCurrentRecordEntries() != null)
				for (ORecordOperation recordEntry : getTransaction().getCurrentRecordEntries()) {
					rid2Records.remove(recordEntry.getRecord().getIdentity());
				}

			if (getTransaction().getAllRecordEntries() != null)
				for (ORecordOperation recordEntry : getTransaction().getAllRecordEntries()) {
					rid2Records.remove(recordEntry.getRecord().getIdentity());
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
	public int getVersion(final Object iPojo) {
		checkOpeness();
		final ODocument record = getRecordByUserObject(iPojo, false);
		if (record != null)
			return record.getVersion();

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
					return iPojo;
				} else
					return OObjectEntityEnhancer.getInstance().getProxiedInstance(iPojo.getClass(), iRecord);
			} else
				return OObjectEntityEnhancer.getInstance().getProxiedInstance(iRecord.getClassName(), entityManager, iRecord);
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

	@Override
	public ODocument getRecordByUserObject(Object iPojo, boolean iCreateIfNotAvailable) {
		if (iPojo instanceof Proxy)
			return OObjectEntitySerializer.getDocument((Proxy) iPojo);
		return OObjectEntitySerializer.getDocument((Proxy) OObjectEntitySerializer.serializeObject(iPojo, this));
	}

	@Override
	public Object getUserObjectByRecord(OIdentifiable iRecord, String iFetchPlan, boolean iCreate) {
		if (!(iRecord instanceof ODocument))
			return null;

		// PASS FOR rid2Records MAP BECAUSE IDENTITY COULD BE CHANGED IF WAS NEW AND IN TX
		ODocument record = rid2Records.get(iRecord.getIdentity());

		if (record == null)
			record = (ODocument) iRecord;

		Object pojo = OObjectEntityEnhancer.getInstance().getProxiedInstance(record.getClassName(), getEntityManager(), record);

		return pojo;
	}

	/**
	 * Register a new POJO
	 */
	@Override
	public void registerUserObject(final Object iObject, final ORecordInternal<?> iRecord) {
		if (!(iRecord instanceof ODocument))
			return;

		final ODocument doc = (ODocument) iRecord;

		if (retainObjects) {

			final ORID rid = iRecord.getIdentity();
			if (rid.isValid())
				rid2Records.put(rid, doc);
		}
	}

	public void registerUserObjectAfterLinkSave(ORecordInternal<?> iRecord) {
	}

	@Override
	public void unregisterPojo(final Object iObject, final ODocument iRecord) {
		if (iRecord != null) {
			final ORID rid = iRecord.getIdentity();
			if (rid.isValid())
				rid2Records.remove(rid);
		}
	}

}
