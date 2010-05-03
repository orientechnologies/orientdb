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

import java.util.HashMap;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandRequest;
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
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.iterator.OObjectIteratorCluster;
import com.orientechnologies.orient.core.iterator.OObjectIteratorMultiCluster;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.object.OObjectSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.OSerializationThreadLocal;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;

@SuppressWarnings("unchecked")
public class ODatabaseObjectTx extends ODatabaseWrapperAbstract<ODatabaseDocument, ODocument> implements ODatabaseObject,
		OUserObject2RecordHandler {

	private HashMap<Object, ODocument>	objects2Records	= new HashMap<Object, ODocument>();
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
		checkSecurity(OUser.CLASS + "." + iClassName, OUser.CREATE);

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
		checkSecurity(OUser.CLASS + "." + iClassName, OUser.READ);

		return new OObjectIteratorMultiCluster<RET>(this, (ODatabaseRecordAbstract<ODocument>) getUnderlying().getUnderlying(),
				getMetadata().getSchema().getClass(iClassName).getClusterIds());
	}

	public <RET> OObjectIteratorCluster<RET> browseCluster(final String iClusterName) {
		checkSecurity(OUser.CLUSTER + "." + iClusterName, OUser.READ);

		return (OObjectIteratorCluster<RET>) new OObjectIteratorCluster<Object>(this,
				(ODatabaseRecordAbstract<ODocument>) getUnderlying().getUnderlying(), getClusterIdByName(iClusterName));
	}

	public ODatabaseObjectTx load(final Object iPojo) {
		if (iPojo == null)
			return this;

		// GET THE ASSOCIATED DOCUMENT
		final ODocument record = getRecordByUserObject(iPojo, true);

		underlying.load(record);

		OObjectSerializerHelper.fromStream(record, iPojo, getEntityManager(), this);

		return this;
	}

	public Object load(final ORID iRecordId) {
		return this;
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
		if (iPojo == null)
			return this;

		OSerializationThreadLocal.INSTANCE.get().clear();

		// GET THE ASSOCIATED DOCUMENT
		ODocument record = objects2Records.get(iPojo);
		if (record == null)
			record = underlying.newInstance(iPojo.getClass().getSimpleName());

		registerPojo(iPojo, record);

		OObjectSerializerHelper.toStream(iPojo, record, getEntityManager(), getMetadata().getSchema().getClass(
				iPojo.getClass().getSimpleName()), this);

		underlying.save(record, iClusterName);

		return this;
	}

	public ODatabaseObject delete(final Object iContent) {
		if (iContent == null)
			return this;

		final ODocument record = getRecordByUserObject(iContent, true);

		underlying.delete(record);

		unregisterPojo(iContent, record);

		return this;
	}

	public long countClass(final String iClassName) {
		return underlying.countClass(iClassName);
	}

	public ODocument getRecordByUserObject(final Object iPojo, final boolean iIsMandatory) {
		ODocument record = objects2Records.get(iPojo);

		if (record == null) {
			if (iIsMandatory)
				throw new OObjectNotManagedException("The object " + iPojo + " is not managed by the current database");

			record = underlying.newInstance(iPojo.getClass().getSimpleName());

			registerPojo(iPojo, record);

			OObjectSerializerHelper.toStream(iPojo, record, getEntityManager(), getMetadata().getSchema().getClass(
					iPojo.getClass().getSimpleName()), this);
		}

		return record;
	}

	public Object getUserObjectByRecord(final ORecordInternal<?> iRecord) {
		if (!(iRecord instanceof ODocument))
			return null;

		ODocument record = (ODocument) iRecord;

		Object pojo = records2Objects.get(iRecord);

		if (pojo == null) {
			try {
				pojo = entityManager.createPojo(record.getClassName());
				registerPojo(pojo, record);

				OObjectSerializerHelper.fromStream(record, pojo, getEntityManager(), this);

			} catch (Exception e) {
				throw new OConfigurationException("Can't retrieve pojo from the record " + record, e);
			}
		}

		return pojo;
	}

	/**
	 * Register a new POJO
	 */
	private void registerPojo(final Object iObject, final ODocument iRecord) {
		if (retainObjects) {
			objects2Records.put(iObject, iRecord);
			records2Objects.put(iRecord, iObject);
		}
	}

	private void unregisterPojo(final Object iObject, final ODocument iRecord) {
		if (iObject != null)
			objects2Records.remove(iObject);

		if (iRecord != null)
			records2Objects.remove(iRecord);
	}

	public ODatabaseObjectTx begin() {
		underlying.begin();
		return this;
	}

	public ODatabaseObjectTx begin(final TXTYPE iStatus) {
		underlying.begin(iStatus);
		return this;
	}

	public ODatabaseObjectTx commit() {
		underlying.commit();
		return this;
	}

	public OMetadata getMetadata() {
		return underlying.getMetadata();
	}

	public ODictionary<Object> getDictionary() {
		if (dictionary == null)
			dictionary = new ODictionaryWrapper(this, underlying);

		return dictionary;
	}

	public ODatabaseObjectTx rollback() {
		underlying.rollback();
		return this;
	}

	public OEntityManager getEntityManager() {
		return entityManager;
	}

	public ODatabaseDocument getUnderlying() {
		return underlying;
	}

	public void setRetainObjects(final boolean iValue) {
		retainObjects = iValue;
	}

	public Object newInstance() {
		return new ODocument(underlying);
	}

	public OCommandRequest command(OCommandRequest iCommand) {
		return null;
	}

	public <RET> OObjectIteratorCluster<RET> query(OQuery<Object> iQuery) {
		return null;
	}
}
