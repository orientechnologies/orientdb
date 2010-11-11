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
import java.util.List;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabasePojoAbstract;
import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.dictionary.ODictionaryWrapper;
import com.orientechnologies.orient.core.entity.OEntityManager;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.iterator.OObjectIteratorCluster;
import com.orientechnologies.orient.core.iterator.OObjectIteratorMultiCluster;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.object.OObjectSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.OSerializationThreadLocal;

/**
 * Object Database instance. It's a wrapper to the class ODatabaseDocumentTx but handle the conversion between ODocument instances
 * and POJOs.
 * 
 * @see ODatabaseDocumentTx
 * @author Luca Garulli
 */
@SuppressWarnings("unchecked")
public class ODatabaseObjectTx extends ODatabasePojoAbstract<ODocument, Object> implements ODatabaseObject,
		OUserObject2RecordHandler {

	private ODictionary<Object>	dictionary;
	private OEntityManager			entityManager;

	public ODatabaseObjectTx(final String iURL) {
		super(new ODatabaseDocumentTx(iURL));
		underlying.setDatabaseOwner(this);
		entityManager = OEntityManager.getEntityManagerByDatabaseURL(iURL);
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
				iClassName);
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

		stream2pojo(record, iPojo, iFetchPlan);

		return this;
	}

	public Object load(final ORID iRecordId) {
		return load(iRecordId, null);
	}

	public Object load(final ORID iRecordId, final String iFetchPlan) {
		checkOpeness();
		if (iRecordId == null)
			return null;

		// GET THE ASSOCIATED DOCUMENT
		final ODocument record = underlying.load(iRecordId);

		if (record == null)
			return null;

		final Object result = stream2pojo(record, newInstance(record.getClassName()), iFetchPlan);
		registerPojo(result, record);
		return result;
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
		pojo2Stream(iPojo, record);

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

	public ODictionary<Object> getDictionary() {
		checkOpeness();
		if (dictionary == null)
			dictionary = new ODictionaryWrapper(this, underlying);

		return dictionary;
	}

	public OEntityManager getEntityManager() {
		return entityManager;
	}

	@Override
	public ODatabaseDocument getUnderlying() {
		return underlying;
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

	public <DBTYPE extends ODatabase> DBTYPE checkSecurity(final String iResource, final byte iOperation) {
		return (DBTYPE) underlying.checkSecurity(iResource, iOperation);
	}

	public <DBTYPE extends ODatabase> DBTYPE checkSecurity(final String iResource, final int iOperation, Object... iResourcesSpecific) {
		return (DBTYPE) underlying.checkSecurity(iResource, iOperation, iResourcesSpecific);
	}

	protected ODocument pojo2Stream(final Object iPojo, final ODocument iRecord) {
		return OObjectSerializerHelper.toStream(iPojo, iRecord, getEntityManager(),
				getMetadata().getSchema().getClass(iPojo.getClass().getSimpleName()), this);
	}

	protected Object stream2pojo(final ODocument record, final Object iPojo, final String iFetchPlan) {
		return OObjectSerializerHelper.fromStream(record, iPojo, getEntityManager(), this, iFetchPlan);
	}
}
