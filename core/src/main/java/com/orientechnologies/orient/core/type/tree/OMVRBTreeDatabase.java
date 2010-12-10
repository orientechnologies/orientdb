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
package com.orientechnologies.orient.core.type.tree;

import java.io.IOException;

import com.orientechnologies.common.collection.OMVRBTreeEntry;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;

/**
 * Persistent MVRB-Tree implementation. The difference with the class OMVRBTreeStorage is the level. In facts this class works
 * directly at the database level, while the other at storage level.
 * 
 */
@SuppressWarnings("serial")
public class OMVRBTreeDatabase<K, V> extends OMVRBTreePersistent<K, V> {
	protected ODatabaseRecord<?>	database;

	public OMVRBTreeDatabase(final ODatabaseRecord<?> iDatabase, final ORID iRID) {
		super(iDatabase.getClusterNameById(iRID.getClusterId()), iRID);
		database = iDatabase;
		record.setDatabase(iDatabase);
	}

	public OMVRBTreeDatabase(final ODatabaseRecord<?> iDatabase, String iClusterName, final OStreamSerializer iKeySerializer,
			final OStreamSerializer iValueSerializer) {
		super(iClusterName, iKeySerializer, iValueSerializer);
		database = iDatabase;
		record.setDatabase(iDatabase);
	}

	@Override
	protected OMVRBTreeEntryDatabase<K, V> createEntry(final K key, final V value) {
		adjustPageSize();
		return new OMVRBTreeEntryDatabase<K, V>(this, key, value, null);
	}

	@Override
	protected OMVRBTreeEntryDatabase<K, V> createEntry(final OMVRBTreeEntry<K, V> parent) {
		adjustPageSize();
		return new OMVRBTreeEntryDatabase<K, V>(parent, parent.getPageSplitItems());
	}

	@Override
	protected OMVRBTreeEntryDatabase<K, V> loadEntry(final OMVRBTreeEntryPersistent<K, V> iParent, final ORID iRecordId)
			throws IOException {

		// SEARCH INTO THE CACHE
		OMVRBTreeEntryDatabase<K, V> entry = (OMVRBTreeEntryDatabase<K, V>) cache.get(iRecordId);
		if (entry == null) {
			// NOT FOUND: CREATE IT AND PUT IT INTO THE CACHE
			entry = new OMVRBTreeEntryDatabase<K, V>(this, (OMVRBTreeEntryDatabase<K, V>) iParent, iRecordId);
			cache.put(iRecordId, entry);
		} else {
			// COULD BE A PROBLEM BECAUSE IF A NODE IS DISCONNECTED CAN BE STAY IN CACHE?
			//entry.load();
			if (iParent != null)
				// FOUND: ASSIGN IT
				entry.setParent(iParent);
		}
		return entry;
	}

	public ODatabaseRecord<?> getDatabase() {
		return database;
	}

	@Override
	public OMVRBTreePersistent<K, V> load() {
		if (!record.getIdentity().isValid())
			// NOTHING TO LOAD
			return this;

		lock.acquireExclusiveLock();

		try {
			usageCounter = 0;

			record.load();
			fromStream(record.toStream());
			return this;

		} finally {
			lock.releaseExclusiveLock();
		}
	}

	@Override
	public OMVRBTreePersistent<K, V> save() throws IOException {
		lock.acquireExclusiveLock();

		try {
			record.save(clusterName);
			return this;
		} finally {
			lock.releaseExclusiveLock();
		}
	}
}
