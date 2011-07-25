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
package com.orientechnologies.orient.core.db;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.object.OLazyObjectList;
import com.orientechnologies.orient.core.db.object.OLazyObjectMap;
import com.orientechnologies.orient.core.db.object.OLazyObjectSet;
import com.orientechnologies.orient.core.db.object.OObjectNotDetachedException;
import com.orientechnologies.orient.core.db.object.OObjectNotManagedException;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.hook.ORecordHook.TYPE;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.object.OObjectSerializerHelper;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;

@SuppressWarnings("unchecked")
public abstract class ODatabasePojoAbstract<T extends Object> extends ODatabaseWrapperAbstract<ODatabaseDocumentTx> implements
		ODatabaseSchemaAware<T> {
	protected HashMap<Integer, ODocument>		objects2Records	= new HashMap<Integer, ODocument>();
	protected IdentityHashMap<ODocument, T>	records2Objects	= new IdentityHashMap<ODocument, T>();
	protected HashMap<ORID, ODocument>			rid2Records			= new HashMap<ORID, ODocument>();
	private boolean													retainObjects		= true;

	public ODatabasePojoAbstract(final ODatabaseDocumentTx iDatabase) {
		super(iDatabase);
		iDatabase.setDatabaseOwner(this);
	}

	public abstract ODocument pojo2Stream(final T iPojo, final ODocument record);

	public abstract Object stream2pojo(final ODocument record, final Object iPojo, final String iFetchPlan);

	@Override
	public void close() {
		objects2Records.clear();
		records2Objects.clear();
		rid2Records.clear();
		super.close();
	}

	public OTransaction getTransaction() {
		return underlying.getTransaction();
	}

	public ODatabaseComplex<T> begin() {
		return (ODatabaseComplex<T>) underlying.begin();
	}

	public ODatabaseComplex<T> begin(final TXTYPE iType) {
		return (ODatabaseComplex<T>) underlying.begin(iType);
	}

	public ODatabaseComplex<T> begin(final OTransaction iTx) {
		return (ODatabaseComplex<T>) underlying.begin(iTx);
	}

	public ODatabaseComplex<T> commit() {
		clearNewEntriesFromCache();

		underlying.commit();

		return this;
	}

	public ODatabaseComplex<T> rollback() {
		clearNewEntriesFromCache();

		underlying.rollback();

		final Set<ORID> rids = new HashSet<ORID>(rid2Records.keySet());

		ORecord<?> record;
		Object object;
		for (ORID rid : rids) {
			if (rid.isTemporary()) {
				record = rid2Records.remove(rid);
				if (record != null) {
					object = records2Objects.remove(record);
					if (object != null) {
						objects2Records.remove(object);
					}
				}
			}
		}

		return this;
	}

	/**
	 * Sets as dirty a POJO. This is useful when you change the object and need to tell to the engine to treat as dirty.
	 * 
	 * @param iPojo
	 *          User object
	 */
	public void setDirty(final Object iPojo) {
		if (iPojo == null)
			return;

		checkOpeness();

		final ODocument record = getRecordByUserObject(iPojo, false);
		if (record == null)
			throw new OObjectNotManagedException("The object " + iPojo + " is not managed by the current database");

		record.setDirty();
	}

	/**
	 * Sets as not dirty a POJO. This is useful when you change some other object and need to tell to the engine to treat this one as
	 * not dirty.
	 * 
	 * @param iPojo
	 *          User object
	 */
	public void unsetDirty(final Object iPojo) {
		if (iPojo == null)
			return;
		checkOpeness();

		final ODocument record = getRecordByUserObject(iPojo, false);
		if (record == null)
			return;

		record.unsetDirty();
	}

	public void setInternal(final ATTRIBUTES attribute, final Object iValue) {
		underlying.setInternal(attribute, iValue);
	}

	/**
	 * Returns the version number of the object. Version starts from 0 assigned on creation.
	 * 
	 * @param iPojo
	 *          User object
	 */
	public int getVersion(final Object iPojo) {
		checkOpeness();

		final ODocument record = getRecordByUserObject(iPojo, false);

		if (record == null)
			throw new OObjectNotManagedException("The object " + iPojo + " is not managed by the current database");

		return record.getVersion();
	}

	/**
	 * Returns the object unique identity.
	 * 
	 * @param iPojo
	 *          User object
	 */
	public ORID getIdentity(final Object iPojo) {
		checkOpeness();

		final ODocument record = getRecordByUserObject(iPojo, false);
		if (record == null)
			throw new OObjectNotManagedException("The object " + iPojo + " is not managed by the current database");

		return record.getIdentity();
	}

	public OUser getUser() {
		return underlying.getUser();
	}

	public OMetadata getMetadata() {
		return underlying.getMetadata();
	}

	/**
	 * Returns a wrapped OCommandRequest instance to catch the result-set by converting it before to return to the user application.
	 */
	public <RET extends OCommandRequest> RET command(final OCommandRequest iCommand) {
		return (RET) new OCommandSQLPojoWrapper(this, underlying.command(iCommand));
	}

	public <RET extends List<?>> RET query(final OQuery<?> iCommand, final Object... iArgs) {
		checkOpeness();

		convertParameters(iArgs);

		final List<ODocument> result = underlying.query(iCommand, iArgs);

		if (result == null)
			return null;

		final List<Object> resultPojo = new ArrayList<Object>();
		Object obj;
		for (ODocument doc : result) {
			// GET THE ASSOCIATED DOCUMENT
			if (doc.getClassName() == null)
				obj = doc;
			else
				obj = getUserObjectByRecord(doc, iCommand.getFetchPlan(), true);

			resultPojo.add(obj);
		}

		return (RET) resultPojo;
	}

	public ODatabaseComplex<T> delete(final ORecordInternal<?> iRecord) {
		underlying.delete((ODocument) iRecord);
		return this;
	}

	public <DBTYPE extends ODatabaseComplex<?>> DBTYPE registerHook(final ORecordHook iHookImpl) {
		underlying.registerHook(iHookImpl);
		return (DBTYPE) this;
	}

	public boolean callbackHooks(final TYPE iType, final OIdentifiable iObject) {
		return underlying.callbackHooks(iType, iObject);
	}

	public Set<ORecordHook> getHooks() {
		return underlying.getHooks();
	}

	public <DBTYPE extends ODatabaseComplex<?>> DBTYPE unregisterHook(final ORecordHook iHookImpl) {
		underlying.unregisterHook(iHookImpl);
		return (DBTYPE) this;
	}

	public boolean isMVCC() {
		return underlying.isMVCC();
	}

	public <DBTYPE extends ODatabaseComplex<?>> DBTYPE setMVCC(final boolean iMvcc) {
		underlying.setMVCC(iMvcc);
		return (DBTYPE) this;
	}

	/**
	 * Specifies if retain handled objects in memory or not. Setting it to false can improve performance on large inserts. Default is
	 * enabled.
	 * 
	 * @param iValue
	 *          True to enable, false to disable it.
	 * @see #isRetainObjects()
	 */
	public ODatabasePojoAbstract<T> setRetainObjects(final boolean iValue) {
		retainObjects = iValue;
		return this;
	}

	/**
	 * Returns true if current configuration retains objects, otherwise false
	 * 
	 * @param iValue
	 *          True to enable, false to disable it.
	 * @see #setRetainObjects(boolean)
	 */

	public boolean isRetainObjects() {
		return retainObjects;
	}

	public ODocument getRecordByUserObject(final Object iPojo, final boolean iCreateIfNotAvailable) {
		checkOpeness();

		if (iPojo instanceof ODocument)
			return (ODocument) iPojo;

		ODocument record = objects2Records.get(System.identityHashCode(iPojo));
		if (record == null) {
			// SEARCH BY RID
			final ORID rid = OObjectSerializerHelper.getObjectID(this, iPojo);
			if (rid != null && rid.isValid()) {
				record = rid2Records.get(rid);
				if (record == null)
					// LOAD IT
					record = underlying.load(rid);
			} else if (iCreateIfNotAvailable) {
				record = underlying.newInstance(iPojo.getClass().getSimpleName());
			} else {
				return null;
			}

			registerUserObject(iPojo, record);
		}

		return record;
	}

	public boolean existsUserObjectByRID(ORID iRID) {
		checkOpeness();
		return rid2Records.containsKey(iRID);
	}

	public ODocument getRecordById(final ORID iRecordId) {
		checkOpeness();
		return iRecordId.isValid() ? rid2Records.get(iRecordId) : null;
	}

	public boolean isManaged(final Object iEntity) {
		return objects2Records.containsKey(System.identityHashCode(iEntity));
	}

	public T getUserObjectByRecord(final ORecordInternal<?> iRecord, final String iFetchPlan) {
		return getUserObjectByRecord(iRecord, iFetchPlan, true);
	}

	public T getUserObjectByRecord(final ORecordInternal<?> iRecord, final String iFetchPlan, final boolean iCreate) {
		if (!(iRecord instanceof ODocument))
			return null;

		// PASS FOR rid2Records MAP BECAUSE IDENTITY COULD BE CHANGED IF WAS NEW AND IN TX
		ODocument record = rid2Records.get(iRecord.getIdentity());

		if (record == null)
			record = (ODocument) iRecord;

		Object pojo = records2Objects.get(record);

		if (pojo == null && iCreate) {
			try {
				// MAKING SURE THAT DATABASE USER IS CURRENT (IN CASE OF DETACHING)
				if (iRecord.getDatabase() != underlying)
					iRecord.setDatabase(underlying);

				if (iRecord.getInternalStatus() == ORecordElement.STATUS.NOT_LOADED)
					record = (ODocument) record.load();

				pojo = newInstance(record.getClassName());
				registerUserObject(pojo, record);

				stream2pojo(record, pojo, iFetchPlan);

			} catch (Exception e) {
				throw new OConfigurationException("Can't retrieve pojo from the record " + record, e);
			}
		}

		return (T) pojo;
	}

	@SuppressWarnings("rawtypes")
	public void attach(final Object iPojo) {
		checkOpeness();

		final ODocument record = objects2Records.get(System.identityHashCode(iPojo));
		if (record != null)
			return;

		if (OObjectSerializerHelper.hasObjectID(iPojo)) {
			for (Field field : iPojo.getClass().getDeclaredFields()) {
				Object value = OObjectSerializerHelper.getFieldValue(iPojo, field.getName());
				if (value instanceof OLazyObjectMap<?>) {
					((OLazyObjectMap) value).assignDatabase(this);
				} else if (value instanceof OLazyObjectList<?>) {
					((OLazyObjectList) value).assignDatabase(this);
				} else if (value instanceof OLazyObjectSet<?>) {
					((OLazyObjectSet) value).assignDatabase(this);
				}
			}
		} else {
			throw new OObjectNotDetachedException("Cannot attach a non detached object");
		}
	}

	/**
	 * Register a new POJO
	 */
	public void registerUserObject(final Object iObject, final ORecordInternal<?> iRecord) {
		if (!(iRecord instanceof ODocument))
			return;

		final ODocument doc = (ODocument) iRecord;

		if (retainObjects) {
			if (iObject != null) {
				objects2Records.put(System.identityHashCode(iObject), doc);
				records2Objects.put(doc, (T) iObject);

				OObjectSerializerHelper.setObjectID(iRecord.getIdentity(), iObject);
				OObjectSerializerHelper.setObjectVersion(iRecord.getVersion(), iObject);
			}

			final ORID rid = iRecord.getIdentity();
			if (rid.isValid())
				rid2Records.put(rid, doc);
		}
	}

	public void unregisterPojo(final T iObject, final ODocument iRecord) {
		if (iObject != null)
			objects2Records.remove(System.identityHashCode(iObject));

		if (iRecord != null) {
			records2Objects.remove(iRecord);

			final ORID rid = iRecord.getIdentity();
			if (rid.isValid())
				rid2Records.remove(rid);
		}
	}

	protected void clearNewEntriesFromCache() {
		for (Iterator<Entry<ORID, ODocument>> it = rid2Records.entrySet().iterator(); it.hasNext();) {
			Entry<ORID, ODocument> entry = it.next();
			if (entry.getKey().isNew()) {
				it.remove();
			}
		}

		for (Iterator<Entry<Integer, ODocument>> it = objects2Records.entrySet().iterator(); it.hasNext();) {
			Entry<Integer, ODocument> entry = it.next();
			if (entry.getValue().getIdentity().isNew()) {
				it.remove();
			}
		}

		for (Iterator<Entry<ODocument, T>> it = records2Objects.entrySet().iterator(); it.hasNext();) {
			Entry<ODocument, T> entry = it.next();
			if (entry.getKey().getIdentity().isNew()) {
				it.remove();
			}
		}
	}

	/**
	 * Convert an array of parameters: if a POJO is used, then replace it with its record id.
	 * 
	 * @param iArgs
	 *          Array of parameters as Object
	 * @see #convertParameter(Object)
	 */
	protected void convertParameters(final Object... iArgs) {
		if (iArgs == null)
			return;

		// FILTER PARAMETERS
		for (int i = 0; i < iArgs.length; ++i)
			iArgs[i] = convertParameter(iArgs[i]);
	}

	/**
	 * Convert a parameter: if a POJO is used, then replace it with its record id.
	 * 
	 * @param iParameter
	 *          Parameter to convert, if applicable
	 * @see #convertParameters(Object...)
	 */
	protected Object convertParameter(final Object iParameter) {
		if (iParameter != null)
			// FILTER PARAMETERS
			if (iParameter instanceof Map<?, ?>) {
				Map<String, Object> map = (Map<String, Object>) iParameter;

				for (Entry<String, Object> e : map.entrySet()) {
					map.put(e.getKey(), convertParameter(e.getValue()));
				}

			} else if (iParameter != null && !OType.isSimpleType(iParameter)) {
				final ORID rid = getIdentity(iParameter);
				if (rid != null && rid.isValid())
					// REPLACE OBJECT INSTANCE WITH ITS RECORD ID
					return rid;
			}

		return iParameter;
	}
}
