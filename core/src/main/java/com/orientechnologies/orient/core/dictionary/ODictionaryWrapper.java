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
package com.orientechnologies.orient.core.dictionary;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Wrapper of dictionary instance that convert values in records.
 */
public class ODictionaryWrapper extends ODictionary<Object> {
	private ODatabaseObject		database;
	private ODatabaseDocument	recordDatabase;

	public ODictionaryWrapper(final ODatabaseObject iDatabase, final ODatabaseDocument iRecordDatabase) {
		super(iRecordDatabase.getMetadata().getIndexManager().getDictionaryIndex());
		this.database = iDatabase;
		this.recordDatabase = iRecordDatabase;
	}

	public boolean containsKey(final String iKey) {
		return recordDatabase.getDictionary().containsKey(iKey);
	}

	@SuppressWarnings("unchecked")
	public <RET extends Object> RET get(final String iKey) {
		return (RET) get(iKey, null);
	}

	@SuppressWarnings("unchecked")
	public <RET extends Object> RET get(final String iKey, final String iFetchPlan) {
		final ORecordInternal<?> record = recordDatabase.getDictionary().get(iKey);
		return (RET) database.getUserObjectByRecord(record, iFetchPlan);
	}

	public void put(final String iKey, final Object iValue) {
		final ODocument record = (ODocument) database.getRecordByUserObject(iValue, false);
		recordDatabase.getDictionary().put(iKey, record);
	}

	public boolean remove(final String iKey) {
		return recordDatabase.getDictionary().remove(iKey);
	}

	public long size() {
		return recordDatabase.getDictionary().size();
	}

	public Iterable<Object> keys() {
		return recordDatabase.getDictionary().keys();
	}
}
