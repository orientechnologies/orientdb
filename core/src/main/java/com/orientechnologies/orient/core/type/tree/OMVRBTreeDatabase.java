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
import java.util.HashSet;
import java.util.Set;

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
	public OMVRBTreeDatabase(final ODatabaseRecord iDatabase, final ORID iRID) {
		super(iDatabase.getClusterNameById(iRID.getClusterId()), iRID);
	}

	public OMVRBTreeDatabase(final ODatabaseRecord iDatabase, String iClusterName, final OStreamSerializer iKeySerializer,
			final OStreamSerializer iValueSerializer) {
		super(iClusterName, iKeySerializer, iValueSerializer);
	}

	public void delete() {
		getDatabase();
		clear();
		record.delete();
	}

	@Override
	protected OMVRBTreeEntryDatabase<K, V> createEntry(final K key, final V value) {
		adjustPageSize();
		final OMVRBTreeEntryDatabase<K, V> node = new OMVRBTreeEntryDatabase<K, V>(this, key, value, null);
		addNodeAsEntrypoint(node);
		return node;
	}

	@Override
	protected OMVRBTreeEntryDatabase<K, V> createEntry(final OMVRBTreeEntry<K, V> parent) {
		adjustPageSize();
		final OMVRBTreeEntryDatabase<K, V> node = new OMVRBTreeEntryDatabase<K, V>(parent, parent.getPageSplitItems());
		addNodeAsEntrypoint(node);
		return node;
	}

	@Override
	protected OMVRBTreeEntryDatabase<K, V> loadEntry(final OMVRBTreeEntryPersistent<K, V> iParent, final ORID iRecordId)
			throws IOException {
		checkForOptimization();

		// SEARCH INTO THE CACHE
		OMVRBTreeEntryDatabase<K, V> entry = (OMVRBTreeEntryDatabase<K, V>) searchNodeInCache(iRecordId);
		if (entry == null) {
			// NOT FOUND: CREATE IT AND PUT IT INTO THE CACHE
			entry = new OMVRBTreeEntryDatabase<K, V>(this, (OMVRBTreeEntryDatabase<K, V>) iParent, iRecordId);
			addNodeInCache(entry);
			addNodeAsEntrypoint(entry);

			// RECONNECT THE LOADED NODE WITH IN-MEMORY PARENT, LEFT AND RIGHT
			if (entry.parent == null && entry.parentRid.isValid()) {
				// TRY TO ASSIGN THE PARENT IN CACHE IF ANY
				final OMVRBTreeEntryPersistent<K, V> parentNode = searchNodeInCache(entry.parentRid);
				if (parentNode != null)
					entry.setParent(parentNode);
			}

			if (entry.left == null && entry.leftRid.isValid()) {
				// TRY TO ASSIGN THE PARENT IN CACHE IF ANY
				final OMVRBTreeEntryPersistent<K, V> leftNode = searchNodeInCache(entry.leftRid);
				if (leftNode != null)
					entry.setLeft(leftNode);
			}

			if (entry.right == null && entry.rightRid.isValid()) {
				// TRY TO ASSIGN THE PARENT IN CACHE IF ANY
				final OMVRBTreeEntryPersistent<K, V> rightNode = searchNodeInCache(entry.rightRid);
				if (rightNode != null)
					entry.setRight(rightNode);
			}

		} else {
			// COULD BE A PROBLEM BECAUSE IF A NODE IS DISCONNECTED CAN IT STAY IN CACHE?
			// entry.load();
			if (iParent != null)
				// FOUND: ASSIGN IT ONLY IF NOT NULL
				entry.setParent(iParent);
		}

		entry.checkEntryStructure();

		return entry;
	}

	@Override
	public OMVRBTreePersistent<K, V> load() {
		if (!record.getIdentity().isValid())
			// NOTHING TO LOAD
			return this;

		getDatabase();
		record.reload();
		record.recycle(this);
		fromStream(record.toStream());
		setLastSearchNode(null, null);
		return this;
	}

	@Override
	public OMVRBTreePersistent<K, V> save() throws IOException {
		getDatabase();
		record.save(clusterName);
		return this;
	}

	public void onAfterTxCommit() {
		final Set<ORID> nodesInMemory = getAllNodesInCache();

		if (nodesInMemory.isEmpty())
			return;

		// FIX THE CACHE CONTENT WITH FINAL RECORD-IDS
		final Set<ORID> keys = new HashSet<ORID>(nodesInMemory);
		OMVRBTreeEntryDatabase<K, V> entry;
		for (ORID rid : keys) {
			if (rid.getClusterPosition() < -1) {
				// FIX IT IN CACHE
				entry = (OMVRBTreeEntryDatabase<K, V>) searchNodeInCache(rid);

				// OVERWRITE IT WITH THE NEW RID
				removeNodeFromCache(rid);
				addNodeInCache(entry);
			}
		}
	}
}
