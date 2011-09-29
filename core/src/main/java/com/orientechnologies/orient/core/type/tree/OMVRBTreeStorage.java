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
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OClusterLogical;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;

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

	protected int						clusterId;

	public OMVRBTreeStorage(final OStorageLocal iStorage, final String iClusterName, final ORID iRID) {
		super(iClusterName, iRID);
		storage = iStorage;
		clusterId = storage.getClusterIdByName(OStorage.CLUSTER_INTERNAL_NAME);
	}

	public OMVRBTreeStorage(final OStorageLocal iStorage, String iClusterName, final OStreamSerializer iKeySerializer,
			final OStreamSerializer iValueSerializer) {
		super(iClusterName, iKeySerializer, iValueSerializer);
		storage = iStorage;
		clusterId = storage.getClusterIdByName(OStorage.CLUSTER_INTERNAL_NAME);
	}

	@Override
	protected OMVRBTreeEntryPersistent<K, V> createEntry(OMVRBTreeEntry<K, V> iParent) {
		adjustPageSize();
		return new OMVRBTreeEntryStorage<K, V>(iParent, iParent.getPageSplitItems());
	}

	@Override
	protected OMVRBTreeEntryPersistent<K, V> createEntry(final K key, final V value) {
		adjustPageSize();
		return new OMVRBTreeEntryStorage<K, V>(this, key, value, null);
	}

	@Override
	protected OMVRBTreeEntryPersistent<K, V> createEntry(OMVRBTreeEntryPersistent<K, V> iParent, ORID iRecordId) throws IOException {
		return new OMVRBTreeEntryStorage<K, V>(this, iParent, iRecordId);
	}

	@Override
	public OMVRBTreePersistent<K, V> load() throws IOException {
		((ORecordId) record.getIdentity()).clusterId = clusterId;
		ORawBuffer raw = storage.readRecord(null, (ORecordId) record.getIdentity(), null);
		if (raw == null)
			throw new OConfigurationException("Can't load map with id " + clusterId + ":" + record.getIdentity().getClusterPosition());
		record.setVersion(raw.version);
		record.recycle(this);
		fromStream(raw.buffer);
		setLastSearchNode(null, null);
		return this;
	}

	@Override
	public OMVRBTreePersistent<K, V> save() throws IOException {
		record.fromStream(toStream());
		if (record.getIdentity().isValid())
			// UPDATE IT WITHOUT VERSION CHECK SINCE ALL IT'S LOCKED
			record.setVersion(storage.updateRecord((ORecordId) record.getIdentity(), record.toStream(), -1, record.getRecordType()));
		else {
			// CREATE IT
			if (record.getIdentity().getClusterId() == ORID.CLUSTER_ID_INVALID)
				((ORecordId) record.getIdentity()).clusterId = clusterId;

			storage.createRecord((ORecordId) record.getIdentity(), record.toStream(), record.getRecordType());
		}
		record.unsetDirty();
		return this;
	}

	public void delete() {
		clear();
		storage.deleteRecord((ORecordId) record.getIdentity(), record.getVersion());
	}
}
