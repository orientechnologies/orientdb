/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.db.object;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabaseWrapperAbstract;
import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.dictionary.ODictionaryWrapper;
import com.orientechnologies.orient.core.entity.OEntityManager;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.hook.ORecordHook.TYPE;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.iterator.OObjectIteratorCluster;
import com.orientechnologies.orient.core.iterator.OObjectIteratorMultiCluster;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.object.OObjectSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.OSerializationThreadLocal;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;

/**
 * Object Database instance. It's a wrapper to the class ODatabaseDocumentTx but handle the conversion between ODocument instances
 * and POJOs.
 * 
 * @see ODatabaseDocumentTx
 * @author Luca Garulli
 */
@SuppressWarnings("unchecked")
public class ODatabaseObjectTx extends ODatabaseWrapperAbstract<ODatabaseDocument, ODocument> implements ODatabaseObject,
		OUserObject2RecordHandler {

	private HashMap<Integer, ODocument>	objects2Records	= new HashMap<Integer, ODocument>();
	private HashMap<ODocument, Object>	records2Objects	= new HashMap<ODocument, Object>();
	private ODictionary<Object>					dictionary;
	private OEntityManager							entityManager		= new OEntityManager();
	private boolean											retainObjects		= true;

	public ODatabaseObjectTx(final String iURL) {
		super(new ODatabaseDocumentTx(iURL));
		underlying.setDatabaseOwner(this);
	}

	@Override
	public void close() {
		objects2Records.clear();
		records2Objects.clear();
		super.close();
	}

	public <T> T newInstance(final Class<T> iType) {
		return (T) newInstance(iType.getName());
	}

	/**
	 * Create a new POJO by its class name. Assure to have called the registerEntityClasses() declaring the packages that are part of
	 * entity classes.
	 * 
	 * @see #registerEntityClasses(String)
	 */
	public Object newInstance(final String iClassName) {
		checkOpeness();
		checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_CREATE, iClassName);

		try {
			return entityManager.createPojo(iClassName);
		} catch (Exception e) {
			OLogManager.instance().error(this, "Error on creating object of class " + iClassName, e, ODatabaseException.class);
		}
		return null;
	}

	public <RET> OObjectIteratorMultiCluster<RET> browseClass(final Class<RET> iClusterClass) {
		if (iClusterClass == null)
			return null;

		return browseClass(iClusterClass.getSimpleName());
	}

	public <RET> OObjectIteratorMultiCluster<RET> browseClass(final String iClassName) {
		checkOpeness();
		checkSecurity(ODatabaseSecurityResources.CLASS, ORole.PERMISSION_READ, iClassName);

		return new OObjectIteratorMultiCluster<RET>(this, (ODatabaseRecordAbstract<ODocument>) getUnderlying().getUnderlying(),
				getMetadata().getSchema().getClass(iClassName).getClusterIds());
	}

	public <RET> OObjectIteratorCluster<RET> browseCluster(final String iClusterName) {
		checkOpeness();
		checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, iClusterName);

		return (OObjectIteratorCluster<RET>) new OObjectIteratorCluster<Object>(this,
				(ODatabaseRecordAbstract<ODocument>) getUnderlying().getUnderlying(), getClusterIdByName(iClusterName));
	}

	public ODatabaseObjectTx load(final Object iPojo) {
		return load(iPojo, null);
	}

	public ODatabaseObjectTx load(final Object iPojo, final String iFetchPlan) {
		checkOpeness();
		if (iPojo == null)
			return this;

		// GET THE ASSOCIATED DOCUMENT
		final ODocument record = getRecordByUserObject(iPojo, true);

		underlying.load(record);

		OObjectSerializerHelper.fromStream(record, iPojo, getEntityManager(), this, iFetchPlan);

		return this;
	}

	public Object load(final ORID iRecordId) {
		return load(iRecordId, null);
	}

	public Object load(final ORID iRecordId, final String iFetchPlan) {
		checkOpeness();
		if (iRecordId == null)
			return this;

		// GET THE ASSOCIATED DOCUMENT
		final ODocument record = underlying.load(iRecordId);

		return record == null ? null : OObjectSerializerHelper.fromStream(record, newInstance(record.getClassName()),
				getEntityManager(), this, iFetchPlan);
	}

	/**
	 * If the record is new and a class was specified, the configured cluster id will be used to store the class.
	 */
	public ODatabaseObject save(final Object iContent) {
		return save(iContent, null);
	}

	/**
	 * Store the record on the specified cluster only after having checked the cluster is allowed and figures in the configured and
	 * the record is valid following the constraints declared in the schema.
	 * 
	 * @see ORecordSchemaAware#validate()
	 */
	public ODatabaseObject save(final Object iPojo, final String iClusterName) {
		checkOpeness();

		if (iPojo == null)
			return this;

		OSerializationThreadLocal.INSTANCE.get().clear();

		// GET THE ASSOCIATED DOCUMENT
		ODocument record = objects2Records.get(System.identityHashCode(iPojo));
		if (record == null)
			record = underlying.newInstance(iPojo.getClass().getSimpleName());

		registerPojo(iPojo, record);

		OObjectSerializerHelper.toStream(iPojo, record, getEntityManager(),
				getMetadata().getSchema().getClass(iPojo.getClass().getSimpleName()), this);

		underlying.save(record, iClusterName);

		return this;
	}

	public ODatabaseObject delete(final Object iContent) {
		checkOpeness();

		if (iContent == null)
			return this;

		final ODocument record = getRecordByUserObject(iContent, true);

		underlying.delete(record);

		unregisterPojo(iContent, record);

		return this;
	}

	public long countClass(final String iClassName) {
		checkOpeness();
		return underlying.countClass(iClassName);
	}

	public ODocument getRecordByUserObject(final Object iPojo, final boolean iIsMandatory) {
		checkOpeness();

		if (iPojo instanceof ODocument)
			return (ODocument) iPojo;

		ODocument record = objects2Records.get(System.identityHashCode(iPojo));

		if (record == null) {
			if (iIsMandatory)
				throw new OObjectNotManagedException("The object " + iPojo + " is not managed by the current database");

			record = underlying.newInstance(iPojo.getClass().getSimpleName());

			registerPojo(iPojo, record);

			OObjectSerializerHelper.toStream(iPojo, record, getEntityManager(),
					getMetadata().getSchema().getClass(iPojo.getClass().getSimpleName()), this);
		}

		return record;
	}

	public boolean existsUserObjectByRecord(ORecordInternal<?> iRecord) {
		checkOpeness();
		if (!(iRecord instanceof ODocument))
			return false;

		return records2Objects.containsKey(iRecord);
	}

	public Object getUserObjectByRecord(final ORecordInternal<?> iRecord, final String iFetchPlan) {
		checkOpeness();
		if (!(iRecord instanceof ODocument))
			return null;

		final ODocument record = (ODocument) iRecord;

		Object pojo = records2Objects.get(iRecord);

		if (pojo == null) {
			try {
				pojo = entityManager.createPojo(record.getClassName());
				registerPojo(pojo, record);

				OObjectSerializerHelper.fromStream(record, pojo, getEntityManager(), this, iFetchPlan);

			} catch (Exception e) {
				throw new OConfigurationException("Can't retrieve pojo from the record " + record, e);
			}
		}

		return pojo;
	}

	public ODatabaseObjectTx begin() {
		checkOpeness();
		underlying.begin();
		return this;
	}

	public ODatabaseObjectTx begin(final TXTYPE iStatus) {
		checkOpeness();
		underlying.begin(iStatus);
		return this;
	}

	public ODatabaseObjectTx commit() {
		checkOpeness();
		underlying.commit();
		return this;
	}

	public OMetadata getMetadata() {
		checkOpeness();
		return underlying.getMetadata();
	}

	public ODictionary<Object> getDictionary() {
		checkOpeness();
		if (dictionary == null)
			dictionary = new ODictionaryWrapper(this, underlying);

		return dictionary;
	}

	public ODatabaseObjectTx rollback() {
		checkOpeness();
		underlying.rollback();
		return this;
	}

	public OEntityManager getEntityManager() {
		return entityManager;
	}

	@Override
	public ODatabaseDocument getUnderlying() {
		return underlying;
	}

	public void setRetainObjects(final boolean iValue) {
		retainObjects = iValue;
	}

	public boolean isRetainObjects() {
		return retainObjects;
	}

	public Object newInstance() {
		checkOpeness();
		return new ODocument(underlying);
	}

	public <RET extends List<?>> RET query(final OQuery<?> iCommand) {
		checkOpeness();
		final List<ODocument> result = underlying.query(iCommand);

		if (result == null)
			return null;

		final List<Object> resultPojo = new ArrayList<Object>();
		Object obj;
		for (ODocument doc : result) {
			// GET THE ASSOCIATED DOCUMENT
			obj = getUserObjectByRecord(doc, iCommand.getFetchPlan());
			resultPojo.add(obj);
		}

		return (RET) resultPojo;
	}

	public OCommandRequest command(final OCommandRequest iCommand) {
		checkOpeness();
		return underlying.command(iCommand);
	}

	public <DBTYPE extends ODatabase> DBTYPE checkSecurity(final String iResource, final byte iOperation) {
		return (DBTYPE) underlying.checkSecurity(iResource, iOperation);
	}

	public <DBTYPE extends ODatabase> DBTYPE checkSecurity(final String iResource, final int iOperation, Object... iResourcesSpecific) {
		return (DBTYPE) underlying.checkSecurity(iResource, iOperation, iResourcesSpecific);
	}

	public Set<ORecordHook> getHooks() {
		checkOpeness();
		return underlying.getHooks();
	}

	public <DB extends ODatabaseComplex<?>> DB registerHook(final ORecordHook iHookImpl) {
		checkOpeness();
		return (DB) underlying.registerHook(iHookImpl);
	}

	public <DB extends ODatabaseComplex<?>> DB unregisterHook(final ORecordHook iHookImpl) {
		checkOpeness();
		return (DB) underlying.unregisterHook(iHookImpl);
	}

	public void callbackHooks(final TYPE iType, final Object iObject) {
		checkOpeness();
		underlying.callbackHooks(iType, iObject);
	}

	public OUser getUser() {
		checkOpeness();
		return underlying.getUser();
	}

	/**
	 * Register a new POJO
	 */
	private void registerPojo(final Object iObject, final ODocument iRecord) {
		if (retainObjects) {
			objects2Records.put(System.identityHashCode(iObject), iRecord);
			records2Objects.put(iRecord, iObject);
		}
	}

	private void unregisterPojo(final Object iObject, final ODocument iRecord) {
		if (iObject != null)
			objects2Records.remove(System.identityHashCode(iObject));

		if (iRecord != null)
			records2Objects.remove(iRecord);
	}
}
