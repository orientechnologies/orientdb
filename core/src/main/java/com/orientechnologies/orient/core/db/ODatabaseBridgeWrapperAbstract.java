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

import java.util.List;
import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.hook.ORecordHook.TYPE;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;

@SuppressWarnings("unchecked")
public abstract class ODatabaseBridgeWrapperAbstract<DB extends ODatabaseComplex<REC>, REC extends ORecordInternal<?>, T extends Object>
		extends ODatabaseWrapperAbstract<DB, REC> implements ODatabaseComplex<T> {

	public ODatabaseBridgeWrapperAbstract(final DB iDatabase) {
		super(iDatabase);
		iDatabase.setDatabaseOwner((ODatabaseComplex<?>) this);
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
		underlying.delete(iRecord);
		return this;
	}

	public ORecordInternal<?> getRecordByUserObject(final Object iUserObject, final boolean iMandatory) {
		if (databaseOwner != this)
			return getDatabaseOwner().getRecordByUserObject(iUserObject, false);

		return (ORecordInternal<?>) iUserObject;
	}

	public Object getUserObjectByRecord(final ORecordInternal<?> iRecord, final String iFetchPlan) {
		if (databaseOwner != this)
			return databaseOwner.getUserObjectByRecord(iRecord, iFetchPlan);

		return iRecord;
	}

	public boolean existsUserObjectByRecord(final ORecordInternal<?> iRecord) {
		if (databaseOwner != this)
			return databaseOwner.existsUserObjectByRecord(iRecord);
		return false;
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
