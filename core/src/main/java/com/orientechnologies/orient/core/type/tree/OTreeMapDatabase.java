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

import com.orientechnologies.common.collection.OTreeMapEntry;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.serialization.OMemoryInputStream;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerFactory;

@SuppressWarnings("serial")
public class OTreeMapDatabase<K, V> extends OTreeMapPersistent<K, V> {
	protected ODatabaseRecord<?>	database;

	public OTreeMapDatabase(final ODatabaseRecord<?> iDatabase, final ORID iRID) {
		super(iDatabase.getClusterNameById(iRID.getClusterId()), iRID);
		database = iDatabase;
		record.setDatabase(iDatabase);
	}

	public OTreeMapDatabase(final ODatabaseRecord<?> iDatabase, String iClusterName, final OStreamSerializer iKeySerializer,
			final OStreamSerializer iValueSerializer) {
		super(iClusterName, iKeySerializer, iValueSerializer);
		database = iDatabase;
		record.setDatabase(iDatabase);
	}

	@Override
	protected OTreeMapEntryDatabase<K, V> createEntry(final K key, final V value) {
		adjustPageSize();
		return new OTreeMapEntryDatabase<K, V>(this, key, value, null);
	}

	@Override
	protected OTreeMapEntryDatabase<K, V> createEntry(final OTreeMapEntry<K, V> parent) {
		adjustPageSize();
		return new OTreeMapEntryDatabase<K, V>(parent, parent.getPageSplitItems());
	}

	@Override
	protected OTreeMapEntryDatabase<K, V> loadEntry(final OTreeMapEntryPersistent<K, V> iParent, final ORID iRecordId)
			throws IOException {

		// SEARCH INTO THE CACHE
		OTreeMapEntryDatabase<K, V> entry = null;// (OTreeMapEntryDatabase<K, V>) cache.get(iRecordId);
		if (entry == null) {
			// NOT FOUND: CREATE IT AND PUT IT INTO THE CACHE
			entry = new OTreeMapEntryDatabase<K, V>(this, (OTreeMapEntryDatabase<K, V>) iParent, iRecordId);
			// cache.put(iRecordId, entry);
		} else {
			// entry.load();

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
	public OTreeMapPersistent<K, V> load() throws IOException {
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
	public OTreeMapPersistent<K, V> save() throws IOException {
		lock.acquireExclusiveLock();

		try {
			record.save(clusterName);
			return this;
		} finally {
			lock.releaseExclusiveLock();
		}
	}

	@Override
	protected void serializerFromStream(final OMemoryInputStream stream) throws IOException {
		keySerializer = OStreamSerializerFactory.get(database, stream.getAsString());
		valueSerializer = OStreamSerializerFactory.get(database, stream.getAsString());
	}
}
