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

import java.util.HashSet;
import java.util.Set;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeMapProvider;

/**
 * Persistent MVRB-Tree implementation. The difference with the class OMVRBTreeStorage is the level. In facts this class works
 * directly at the database level, while the other at storage level.
 * 
 */
@SuppressWarnings("serial")
public class OMVRBTreeDatabase<K, V> extends OMVRBTreePersistent<K, V> {

	public OMVRBTreeDatabase(final ODatabaseRecord iDatabase, final ORID iRID) {
		super(new OMVRBTreeMapProvider<K, V>(null, iDatabase.getClusterNameById(iRID.getClusterId()), iRID));
	}

	public OMVRBTreeDatabase(String iClusterName, final OBinarySerializer iKeySerializer,
													 final OStreamSerializer iValueSerializer, int keySize) {
		super(new OMVRBTreeMapProvider<K, V>(null, iClusterName, iKeySerializer, iValueSerializer), keySize);
	}

	public void onAfterTxCommit() {
		final Set<ORID> nodesInMemory = getAllNodesInCache();

		if (nodesInMemory.isEmpty())
			return;

		// FIX THE CACHE CONTENT WITH FINAL RECORD-IDS
		final Set<ORID> keys = new HashSet<ORID>(nodesInMemory);
		OMVRBTreeEntryPersistent<K, V> entry;
		for (ORID rid : keys) {
			if (rid.getClusterPosition() < -1) {
				// FIX IT IN CACHE
				entry = (OMVRBTreeEntryPersistent<K, V>) searchNodeInCache(rid);

				// OVERWRITE IT WITH THE NEW RID
				removeNodeFromCache(rid);
				addNodeInCache(entry);
			}
		}
	}
}
