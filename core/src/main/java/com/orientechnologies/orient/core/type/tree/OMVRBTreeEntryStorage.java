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
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.ORawBuffer;

/**
 * Persistent TreeMap implementation that use a OStorage instance to handle the entries. This class can't be used also from the
 * user. It's not transaction aware.
 * 
 * @author Luca Garulli
 * 
 * @param <K>
 *          Key type
 * @param <V>
 *          Value type
 */
@SuppressWarnings("serial")
public class OMVRBTreeEntryStorage<K, V> extends OMVRBTreeEntryPersistent<K, V> {

	public OMVRBTreeEntryStorage(OMVRBTreeEntry<K, V> iParent, int iPosition) {
		super(iParent, iPosition);
		record.setIdentity(pTree.getRecord().getIdentity().getClusterId(), ORID.CLUSTER_POS_INVALID);
	}

	public OMVRBTreeEntryStorage(OMVRBTreePersistent<K, V> iTree, K key, V value, OMVRBTreeEntryPersistent<K, V> iParent) {
		super(iTree, key, value, iParent);
		record.setIdentity(pTree.getRecord().getIdentity().getClusterId(), ORID.CLUSTER_POS_INVALID);
	}

	public OMVRBTreeEntryStorage(OMVRBTreePersistent<K, V> iTree, OMVRBTreeEntryPersistent<K, V> iParent, ORID iRecordId)
			throws IOException {
		super(iTree, iParent, iRecordId);
	}

	@Override
	public OMVRBTreeEntryStorage<K, V> load() throws IOException {
		ORawBuffer raw = ((OMVRBTreeStorage<K, V>) tree).storage.readRecord(null, (ORecordId) record.getIdentity(), null);

		record.setVersion(raw.version);

		fromStream(raw.buffer);
		super.load();
		return this;
	}

	@Override
	public OMVRBTreeEntryStorage<K, V> save() {
		record.fromStream(toStream());

		if (record.getIdentity().isValid())
			// UPDATE IT WITHOUT VERSION CHECK SINCE ALL IT'S LOCKED
			record.setVersion(((OMVRBTreeStorage<K, V>) tree).storage.updateRecord((ORecordId) record.getIdentity(), record.toStream(),
					-1, record.getRecordType()));
		else {
			// CREATE IT
			record.setIdentity(
					record.getIdentity().getClusterId(),
					((OMVRBTreeStorage<K, V>) tree).storage.createRecord((ORecordId) record.getIdentity(), record.toStream(),
							record.getRecordType()));
		}
		record.unsetDirty();

		super.save();

		return this;
	}

	/**
	 * Delete all the nodes recursively. IF they are not loaded in memory, load all the tree.
	 * 
	 * @throws IOException
	 */
	@Override
	public OMVRBTreeEntryStorage<K, V> delete() throws IOException {
		// EARLY LOAD LEFT AND DELETE IT RECURSIVELY
		if (getLeft() != null)
			((OMVRBTreeEntryPersistent<K, V>) getLeft()).delete();

		// EARLY LOAD RIGHT AND DELETE IT RECURSIVELY
		if (getRight() != null)
			((OMVRBTreeEntryPersistent<K, V>) getRight()).delete();

		// DELETE MYSELF
		((OMVRBTreeStorage<K, V>) tree).storage.deleteRecord((ORecordId) record.getIdentity(), record.getVersion());

		// FORCE REMOVING OF K/V AND SEIALIZED K/V AS WELL
		keys = null;
		values = null;

		super.delete();
		return this;
	}

	@Override
	protected Object keyFromStream(final int iIndex) throws IOException {
		return pTree.keySerializer.fromStream(null, inStream.getAsByteArray(serializedKeys[iIndex]));
	}

	@Override
	protected Object valueFromStream(final int iIndex) throws IOException {
		return pTree.valueSerializer.fromStream(null, inStream.getAsByteArray(serializedValues[iIndex]));
	}
}
