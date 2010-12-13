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
package com.orientechnologies.orient.client.dictionary;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.dictionary.ODictionaryAbstract;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;

@SuppressWarnings("unchecked")
public class ODictionaryClient<T extends Object> extends ODictionaryAbstract<T> {
	private OStorageRemote			storage;
	private ODatabaseRecord<?>	database;

	public ODictionaryClient(final ODatabaseRecord<?> iDatabase, final OStorageRemote iStorage) {
		super(iDatabase);
		this.database = iDatabase;
		this.storage = iStorage;
	}

	public T get(final Object iKey) {
		return get(iKey, null);
	}

	public T get(final Object iKey, final String iFetchPlan) {
		if (iKey == null)
			return null;

		ORecordInternal<?> record = storage.dictionaryLookup(database, (String) iKey);

		return (T) database.getUserObjectByRecord(record, iFetchPlan);
	}

	public ORecordInternal<?> putRecord(String iKey, ORecordInternal<?> iValue) {
		return (ORecordInternal<?>) put(iKey, iValue);
	}

	public T put(final String iKey, final Object iValue) {
		if (iKey == null)
			return null;

		ORecordInternal<?> record = database.getRecordByUserObject(iValue, false);
		if (record.isDirty())
			record.save();

		ORecordInternal<?> oldRecord = null;
		if (record.getIdentity().isTemporary()) {
			// DURING TRANSACTION, KEEP IT IN MEMORY
			if (transactionalEntries == null)
				transactionalEntries = new HashMap<String, ORecord<?>>();

			// REMEMBER THE KEY TO RE-SET WHEN THE TX IS COMMITTED AND RID ARE NOT MORE TEMPORARIES
			transactionalEntries.put(iKey, record);
			oldRecord = null;
		} else
			// SEND TO THE SERVER
			oldRecord = storage.dictionaryPut(database, iKey, record);

		return (T) database.getUserObjectByRecord(oldRecord, null);
	}

	public T remove(final Object iKey) {
		if (iKey == null)
			return null;

		return (T) database.getUserObjectByRecord(storage.dictionaryRemove(database, iKey), null);
	}

	public boolean containsKey(final Object iKey) {
		return storage.dictionaryLookup(database, (String) iKey) != null;
	}

	public int size() {
		return storage.dictionarySize(database);
	}

	/**
	 * No implementation since you can't create a database via common APIs.
	 */
	public void create() {
	}

	public void load() {
	}

	/**
	 * Preload all the keys to fetch values in lazy way.
	 */
	public Iterator<Entry<String, T>> iterator() {
		return new ODictionaryClientIterator<T>(database, storage.dictionaryKeys());
	}

	public Set<String> keySet() {
		return storage.dictionaryKeys();
	}
}
