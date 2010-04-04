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

import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.db.vobject.ODatabaseVObject;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ORecordVObject;

/**
 * Wrapper of dictionary instance that convert values in records.
 */
public class ODictionaryWrapper implements ODictionary<Object> {
	private ODatabaseObject		database;
	private ODatabaseVObject	recordDatabase;

	public ODictionaryWrapper(final ODatabaseObject iDatabase, final ODatabaseVObject iRecordDatabase) {
		this.database = iDatabase;
		this.recordDatabase = iRecordDatabase;
	}

	public boolean containsKey(final Object iKey) {
		return recordDatabase.getDictionary().containsKey(iKey);
	}

	public Object get(final Object iKey) {
		ORecord<?> record = recordDatabase.getDictionary().get(iKey);

		return database.getUserObjectByRecord(record);
	}

	public Object put(final String iKey, final Object iValue) {
		ORecordVObject record = (ORecordVObject) database.getRecordByUserObject(iValue, false);

		ORecord<?> oldRecord = recordDatabase.getDictionary().put(iKey, (ORecordVObject) record);

		return database.getUserObjectByRecord(oldRecord);
	}

	public Object remove(final Object iKey) {
		ORecord<?> record = recordDatabase.getDictionary().remove(iKey);

		return database.getUserObjectByRecord(record);
	}

	public Iterator<Entry<String, Object>> iterator() {
		return new ODictionaryIteratorWrapper(recordDatabase, (ODictionaryIterator<ORecordVObject>) recordDatabase.getDictionary()
				.iterator());
	}

	public int size() {
		return recordDatabase.getDictionary().size();
	}

	public Set<String> keySet() {
		return recordDatabase.getDictionary().keySet();
	}
}
