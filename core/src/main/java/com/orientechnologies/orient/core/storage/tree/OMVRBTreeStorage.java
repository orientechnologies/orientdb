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

import com.orientechnologies.common.collection.OMVRBTreeEntry;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.serialization.OMemoryInputStream;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerFactory;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.impl.local.OClusterLogical;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeEntryPersistent;
import com.orientechnologies.orient.core.type.tree.OMVRBTreePersistent;

/**
 * Persistent MVRB-Tree implementation. The difference with the class OMVRBTreeDatabase is the level. In facts this class works
 * directly at the storage level, while the other at database level. This class is used for Logical Clusters. It can'be
 * transactional.
 * 
 * @see OClusterLogical
 */
@SuppressWarnings("serial")
public class OMVRBTreeStorage<K, V> extends OMVRBTreePersistent<K, V> {
	protected OStorageLocal	storage;
	int											clusterId;

	public OMVRBTreeStorage(final OStorageLocal iStorage, final String iClusterName, final ORID iRID) {
		super(iClusterName, iRID);
		storage = iStorage;
		clusterId = storage.getClusterIdByName(OStorageLocal.CLUSTER_INTERNAL_NAME);
	}

	public OMVRBTreeStorage(final OStorageLocal iStorage, String iClusterName, final OStreamSerializer iKeySerializer,
			final OStreamSerializer iValueSerializer) {
		super(iClusterName, iKeySerializer, iValueSerializer);
		storage = iStorage;
		clusterId = storage.getClusterIdByName(OStorageLocal.CLUSTER_INTERNAL_NAME);
	}

	@Override
	protected OMVRBTreeEntryPersistent<K, V> createEntry(OMVRBTreeEntry<K, V> iParent) {
		return new OMVRBTreeEntryStorage<K, V>(iParent, iParent.getPageSplitItems());
	}

	@Override
	protected OMVRBTreeEntryPersistent<K, V> createEntry(final K key, final V value) {
		adjustPageSize();
		return new OMVRBTreeEntryStorage<K, V>(this, key, value, null);
	}

	@Override
	protected OMVRBTreeEntryStorage<K, V> loadEntry(OMVRBTreeEntryPersistent<K, V> iParent, ORID iRecordId) throws IOException {
		OMVRBTreeEntryStorage<K, V> entry = null;// (OMVRBTreeEntryStorage<K, V>) cache.get(iRecordId);
		if (entry == null) {
			// NOT FOUND: CREATE IT AND PUT IT INTO THE CACHE
			entry = new OMVRBTreeEntryStorage<K, V>(this, iParent, iRecordId);
			// cache.put(iRecordId, entry);
		} else
			// FOUND: ASSIGN IT
			entry.setParent(iParent);

		return entry;
	}

	@Override
	public OMVRBTreePersistent<K, V> load() throws IOException {
		lock.acquireExclusiveLock();

		try {
			usageCounter = 0;
			ORawBuffer raw = storage.readRecord(null, -1, clusterId, record.getIdentity().getClusterPosition(), null);

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
	public OMVRBTreePersistent<K, V> save() throws IOException {
		lock.acquireExclusiveLock();

		try {
			record.fromStream(toStream());
			if (record.getIdentity().isValid())
				// UPDATE IT WITHOUT VERSION CHECK SINCE ALL IT'S LOCKED
				record.setVersion(storage.updateRecord(0, record.getIdentity().getClusterId(), record.getIdentity().getClusterPosition(),
						record.toStream(), -1, record.getRecordType()));
			else {
				// CREATE IT
				int cluster = record.getIdentity().getClusterId() == ORID.CLUSTER_ID_INVALID ? clusterId : record.getIdentity()
						.getClusterId();
				record.setIdentity(cluster, storage.createRecord(cluster, record.toStream(), record.getRecordType()));
			}
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
