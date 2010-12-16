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

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.hook.ORecordHook.TYPE;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;

@SuppressWarnings("unchecked")
public abstract class ODatabaseRecordWrapperAbstract<DB extends ODatabaseRecord<REC>, REC extends ORecordInternal<?>> extends
		ODatabaseWrapperAbstract<DB, REC> implements ODatabaseComplex<REC> {

	public ODatabaseRecordWrapperAbstract(final DB iDatabase) {
		super(iDatabase);
		iDatabase.setDatabaseOwner((ODatabaseComplex<?>) this);
	}

	public OTransaction<?> getTransaction() {
		return underlying.getTransaction();
	}

	public ODatabaseComplex<REC> begin() {
		return (ODatabaseComplex<REC>) underlying.begin();
	}

	public ODatabaseComplex<REC> begin(final TXTYPE iType) {
		return (ODatabaseComplex<REC>) underlying.begin(iType);
	}

	public ODatabaseComplex<REC> commit() {
		return (ODatabaseComplex<REC>) underlying.commit();
	}

	public ODatabaseComplex<REC> rollback() {
		return (ODatabaseComplex<REC>) underlying.rollback();
	}

	public OUser getUser() {
		return underlying.getUser();
	}

	public OMetadata getMetadata() {
		return underlying.getMetadata();
	}

	public ODictionary<REC> getDictionary() {
		return underlying.getDictionary();
	}

	public Class<? extends REC> getRecordType() {
		return underlying.getRecordType();
	}

	public Iterator<REC> browseCluster(final String iClusterName) {
		return underlying.browseCluster(iClusterName);
	}

	public <RET extends OCommandRequest> RET command(final OCommandRequest iCommand) {
		return (RET) underlying.command(iCommand);
	}

	public <RET extends List<?>> RET query(final OQuery<? extends Object> iCommand) {
		return (RET) underlying.query(iCommand);
	}

	public REC newInstance() {
		return underlying.newInstance();
	}

	public ODatabaseComplex<REC> delete(final REC iRecord) {
		underlying.delete(iRecord);
		return this;
	}

	public REC load(final ORID iRecordId) {
		return underlying.load(iRecordId);
	}

	public REC load(final REC iRecord) {
		return underlying.load(iRecord);
	}

	public REC load(final REC iRecord, final String iFetchPlan) {
		return underlying.load(iRecord, iFetchPlan);
	}

	public ODatabaseComplex<REC> save(final REC iRecord, final String iClusterName) {
		underlying.save(iRecord, iClusterName);
		return this;
	}

	public ODatabaseComplex<REC> save(final REC iRecord) {
		underlying.save(iRecord);
		return this;
	}

	public boolean isRetainRecords() {
		return underlying.isRetainRecords();
	}

	public ODatabaseRecord<?> setRetainRecords(boolean iValue) {
		underlying.setRetainRecords(iValue);
		return (ODatabaseRecord<?>) this;
	}

	public ORecordInternal<?> getRecordByUserObject(final Object iUserObject, final boolean iMandatory) {
		if (databaseOwner != this)
			return getDatabaseOwner().getRecordByUserObject(iUserObject, false);

		return (ORecordInternal<?>) iUserObject;
	}

	public void registerPojo(final Object iObject, final ODocument iRecord) {
		if (databaseOwner != this)
			getDatabaseOwner().registerPojo(iObject, iRecord);
	}

	public Object getUserObjectByRecord(final ORecordInternal<?> iRecord, final String iFetchPlan) {
		if (databaseOwner != this)
			return databaseOwner.getUserObjectByRecord(iRecord, iFetchPlan);

		return iRecord;
	}

	public boolean existsUserObjectByRID(final ORID iRID) {
		if (databaseOwner != this)
			return databaseOwner.existsUserObjectByRID(iRID);
		return false;
	}

	public <DBTYPE extends ODatabaseRecord<?>> DBTYPE checkSecurity(final String iResource, final int iOperation) {
		return (DBTYPE) underlying.checkSecurity(iResource, iOperation);
	}

	public <DBTYPE extends ODatabaseRecord<?>> DBTYPE checkSecurity(final String iResourceGeneric, final int iOperation,
			final Object... iResourcesSpecific) {
		return (DBTYPE) underlying.checkSecurity(iResourceGeneric, iOperation, iResourcesSpecific);
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
}
