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
package com.orientechnologies.orient.core.storage.tree;

import java.io.IOException;

import com.orientechnologies.common.collection.OTreeMapEntry;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.serialization.OMemoryInputStream;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerFactory;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.impl.local.OClusterLogical;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;
import com.orientechnologies.orient.core.type.tree.OTreeMapEntryPersistent;
import com.orientechnologies.orient.core.type.tree.OTreeMapPersistent;

/**
 * Persistent TreeMap implementation. The difference with the class OTreeMapPersistent is the level. In facts this class works
 * directly at the storage level, while the other at database level. This class is used for Logical Clusters. It can'be
 * transactional.
 * 
 * @see OClusterLogical
 */
@SuppressWarnings("serial")
public class OTreeMapStorage<K, V> extends OTreeMapPersistent<K, V> {
	protected OStorageLocal	storage;
	int											clusterId;

	public OTreeMapStorage(final OStorageLocal iStorage, final String iClusterName, final ORID iRID) {
		super(iClusterName, iRID);
		storage = iStorage;
	}

	public OTreeMapStorage(final OStorageLocal iStorage, String iClusterName, final OStreamSerializer iKeySerializer,
			final OStreamSerializer iValueSerializer) {
		super(iClusterName, iKeySerializer, iValueSerializer);
		storage = iStorage;
	}

	@Override
	protected OTreeMapEntryPersistent<K, V> createEntry(OTreeMapEntry<K, V> iParent) {
		return new OTreeMapEntryStorage<K, V>(iParent, iParent.getPageSplitItems());
	}

	@Override
	protected OTreeMapEntryPersistent<K, V> createEntry(final K key, final V value) {
		adjustPageSize();
		return new OTreeMapEntryStorage<K, V>(this, key, value, null);
	}

	@Override
	protected OTreeMapEntryStorage<K, V> createEntry(OTreeMapEntryPersistent<K, V> iParent, ORID iRecordId) throws IOException {
		return new OTreeMapEntryStorage<K, V>(this, iParent, iRecordId);
	}

	@Override
	public OTreeMapPersistent<K, V> load() throws IOException {
		lock.acquireExclusiveLock();

		try {
			ORawBuffer raw = storage.readRecord(-1, clusterId, record.getIdentity().getClusterPosition());

			if (raw == null)
				throw new OConfigurationException("Can't load map with id " + clusterId + ":" + record.getIdentity().getClusterPosition());

			record.setVersion(raw.version);

			fromStream(raw.buffer);
			return this;

		} finally {
			lock.releaseExclusiveLock();
		}
	}

	@Override
	public OTreeMapPersistent<K, V> save() throws IOException {
		lock.acquireExclusiveLock();

		try {
			record.fromStream(toStream());
			if (record.getIdentity().isValid())
				// UPDATE IT
				record.setVersion(storage.updateRecord(0, clusterId, record.getIdentity().getClusterPosition(), record.toStream(),
						record.getVersion(), record.getRecordType()));
			else
				// CREATE IT
				record.setIdentity(clusterId,
						storage.createRecord(record.getIdentity().getClusterId(), record.toStream(), record.getRecordType()));
			record.unsetDirty();

			return this;

		} finally {
			lock.releaseExclusiveLock();
		}
	}

	@Override
	public void clear() {
		storage.deleteRecord(0, record.getIdentity().getClusterId(), record.getIdentity().getClusterPosition(), record.getVersion());
		root = null;
		super.clear();
	}

	@Override
	protected void serializerFromStream(final OMemoryInputStream stream) throws IOException {
		keySerializer = OStreamSerializerFactory.get(null, stream.getAsString());
		valueSerializer = OStreamSerializerFactory.get(null, stream.getAsString());
	}
}
