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

import java.util.HashMap;
import java.util.List;
import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.object.OObjectNotManagedException;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.hook.ORecordHook.TYPE;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.ORecord.STATUS;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;

@SuppressWarnings("unchecked")
public abstract class ODatabasePojoAbstract<REC extends ORecordInternal<?>, T extends Object> extends
		ODatabaseWrapperAbstract<ODatabaseDocumentTx, REC> implements ODatabaseComplex<T> {
	protected HashMap<Integer, ODocument>	objects2Records	= new HashMap<Integer, ODocument>();
	protected HashMap<ODocument, T>				records2Objects	= new HashMap<ODocument, T>();
	protected HashMap<ORID, ODocument>		rid2Records			= new HashMap<ORID, ODocument>();
	private boolean												retainObjects		= true;

	public ODatabasePojoAbstract(final ODatabaseDocumentTx iDatabase) {
		super(iDatabase);
		iDatabase.setDatabaseOwner((ODatabaseComplex<?>) this);
	}

	protected abstract ODocument pojo2Stream(final T iPojo, final ODocument record);

	protected abstract Object stream2pojo(final ODocument record, final T iPojo, final String iFetchPlan);

	public abstract T newInstance(final String iClassName);

	@Override
	public void close() {
		objects2Records.clear();
		records2Objects.clear();
		rid2Records.clear();
		super.close();
	}

	public ODatabaseComplex<T> begin() {
		return (ODatabaseComplex<T>) underlying.begin();
	}

	public ODatabaseComplex<T> begin(final TXTYPE iType) {
		return (ODatabaseComplex<T>) underlying.begin(iType);
	}

	public ODatabaseComplex<T> commit() {
		return (ODatabaseComplex<T>) underlying.commit();
	}

	public ODatabaseComplex<T> rollback() {
		return (ODatabaseComplex<T>) underlying.rollback();
	}

	public OUser getUser() {
		return underlying.getUser();
	}

	public OMetadata getMetadata() {
		return underlying.getMetadata();
	}

	public <RET extends OCommandRequest> RET command(final OCommandRequest iCommand) {
		return (RET) underlying.command(iCommand);
	}

	public <RET extends List<?>> RET query(final OQuery<? extends Object> iCommand) {
		return (RET) underlying.query(iCommand);
	}

	public ODatabaseComplex<T> delete(final REC iRecord) {
		underlying.delete((ODocument) iRecord);
		return this;
	}

	public <DBTYPE extends ODatabaseComplex<?>> DBTYPE registerHook(final ORecordHook iHookImpl) {
		underlying.registerHook(iHookImpl);
		return (DBTYPE) this;
	}

	public void callbackHooks(final TYPE iType, final Object iObject) {
		underlying.callbackHooks(iType, iObject);
	}

	public Set<ORecordHook> getHooks() {
		return underlying.getHooks();
	}

	public <DBTYPE extends ODatabaseComplex<?>> DBTYPE unregisterHook(final ORecordHook iHookImpl) {
		underlying.unregisterHook(iHookImpl);
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
	public ODatabasePojoAbstract<REC, T> setRetainObjects(final boolean iValue) {
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

	public ODocument getRecordByUserObject(final Object iPojo, final boolean iIsMandatory) {
		checkOpeness();

		if (iPojo instanceof ODocument)
			return (ODocument) iPojo;

		ODocument record = objects2Records.get(System.identityHashCode(iPojo));

		if (record == null) {
			if (iIsMandatory)
				throw new OObjectNotManagedException("The object " + iPojo + " is not managed by the current database");

			record = underlying.newInstance(iPojo.getClass().getSimpleName());

			registerPojo((T) iPojo, record);
			pojo2Stream((T) iPojo, record);
		}

		return record;
	}

	public boolean existsUserObjectByRecord(ORecordInternal<?> iRecord) {
		checkOpeness();
		if (!(iRecord instanceof ODocument))
			return false;

		return records2Objects.containsKey(iRecord);
	}

	public ODocument getRecordById(final ORID iRecordId) {
		checkOpeness();
		return iRecordId.isValid() ? rid2Records.get(iRecordId) : null;
	}

	public T getUserObjectByRecord(final ORecordInternal<?> iRecord, final String iFetchPlan) {
		checkOpeness();
		if (!(iRecord instanceof ODocument))
			return null;

		final ODocument record = (ODocument) iRecord;

		T pojo = records2Objects.get(iRecord);

		if (pojo == null) {
			try {
				if (record.getInternalStatus() == STATUS.NOT_LOADED)
					record.load();

				pojo = newInstance(record.getClassName());
				registerPojo(pojo, record);

				stream2pojo(record, pojo, iFetchPlan);

			} catch (Exception e) {
				throw new OConfigurationException("Can't retrieve pojo from the record " + record, e);
			}
		}

		return pojo;
	}

	/**
	 * Register a new POJO
	 */
	public void registerPojo(final T iObject, final ODocument iRecord) {
		if (retainObjects) {
			objects2Records.put(System.identityHashCode(iObject), iRecord);
			records2Objects.put(iRecord, iObject);

			final ORID rid = iRecord.getIdentity();
			if (rid.isValid())
				rid2Records.put(rid, iRecord);
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
}
