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

import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Wrapper of dictionary instance that convert values in records.
 */
public class ODictionaryWrapper implements ODictionary<Object> {
	private ODatabaseObject		database;
	private ODatabaseDocument	recordDatabase;

	public ODictionaryWrapper(final ODatabaseObject iDatabase, final ODatabaseDocument iRecordDatabase) {
		this.database = iDatabase;
		this.recordDatabase = iRecordDatabase;
	}

	public boolean containsKey(final Object iKey) {
		return recordDatabase.getDictionary().containsKey(iKey);
	}

	public Object get(final Object iKey) {
		ORecordInternal<?> record = recordDatabase.getDictionary().get(iKey);

		return database.getUserObjectByRecord(record);
	}

	public ORecordInternal<?> putRecord(String iKey, ORecordInternal<?> iValue) {
		return (ORecordInternal<?>) put(iKey, iValue);
	}

	public Object put(final String iKey, final Object iValue) {
		ODocument record = (ODocument) database.getRecordByUserObject(iValue, false);

		ORecordInternal<?> oldRecord = recordDatabase.getDictionary().put(iKey, (ODocument) record);

		return database.getUserObjectByRecord(oldRecord);
	}

	public Object remove(final Object iKey) {
		ORecordInternal<?> record = recordDatabase.getDictionary().remove(iKey);

		return database.getUserObjectByRecord(record);
	}

	public Iterator<Entry<String, Object>> iterator() {
		return new ODictionaryIteratorWrapper(recordDatabase, (ODictionaryIterator<ODocument>) recordDatabase.getDictionary()
				.iterator());
	}

	public int size() {
		return recordDatabase.getDictionary().size();
	}

	public Set<String> keySet() {
		return recordDatabase.getDictionary().keySet();
	}
}
