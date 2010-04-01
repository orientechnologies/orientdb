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

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;

@SuppressWarnings("unchecked")
public abstract class ODatabaseRecordWrapperAbstract<DB extends ODatabaseRecord<REC>, REC extends ORecordInternal<?>> extends
		ODatabaseWrapperAbstract<DB, REC> {

	public ODatabaseRecordWrapperAbstract(final DB iDatabase) {
		super(iDatabase);
		iDatabase.setDatabaseOwner((ODatabaseRecord<?>) this);
	}

	public ODatabaseRecord<REC> begin() {
		return (ODatabaseRecord<REC>) underlying.begin();
	}

	public ODatabaseRecord<REC> begin(TXTYPE iType) {
		return (ODatabaseRecord<REC>) underlying.begin(iType);
	}

	public ODatabaseRecord<REC> commit() {
		return (ODatabaseRecord<REC>) underlying.commit();
	}

	public ODatabaseRecord<REC> rollback() {
		return (ODatabaseRecord<REC>) underlying.rollback();
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

	public Iterator<REC> browseCluster(String iClusterName) {
		return underlying.browseCluster(iClusterName);
	}

	public OQuery<REC> query(OQuery<REC> iQuery) {
		// OQueryInternal<REC> query = (OQueryInternal<REC>) iQuery;
		// query.setRecord(((ODatabaseRecord<REC>) getDatabaseOwner()).newInstance());

		return underlying.query(iQuery);
	}

	public REC newInstance() {
		return underlying.newInstance();
	}

	public ODatabaseRecordWrapperAbstract<?, ?> delete(REC iRecord) {
		underlying.delete(iRecord);
		return this;
	}

	public REC load(ORID iRecordId) {
		return underlying.load(iRecordId);
	}

	public REC load(REC iRecord) {
		return underlying.load(iRecord);
	}

	public ODatabaseRecordWrapperAbstract<?, ?> save(REC iRecord, String iClusterName) {
		underlying.save(iRecord, iClusterName);
		return this;
	}

	public ODatabaseRecordWrapperAbstract<?, ?> save(REC iRecord) {
		underlying.save(iRecord);
		return this;
	}

	public ORecord<?> getRecordByUserObject(final Object iUserObject, boolean iMandatory) {
		if (databaseOwner != this)
			return getDatabaseOwner().getRecordByUserObject(iUserObject, false);

		return (ORecord<?>) iUserObject;
	}

	public Object getUserObjectByRecord(final ORecord<?> iRecord) {
		if (databaseOwner != this)
			return getDatabaseOwner().getUserObjectByRecord(iRecord);

		return iRecord;
	}
}
