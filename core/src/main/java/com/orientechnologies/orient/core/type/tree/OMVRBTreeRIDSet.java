/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Kersion 2.0 (the "License");
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
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeProvider;
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeRIDProvider;

/**
 * Persistent MVRB-Tree Set implementation.
 * 
 */
@SuppressWarnings("serial")
public class OMVRBTreeRIDSet extends OMVRBTreePersistent<ORecordId, ORecordId> {

	public OMVRBTreeRIDSet(OMVRBTreeProvider<ORecordId, ORecordId> iProvider) {
		super(iProvider);
	}

	public OMVRBTreeRIDSet(final ODatabaseRecord iDatabase, final ORID iRID) {
		super(new OMVRBTreeRIDProvider(null, iDatabase.getClusterNameById(iRID.getClusterId()), iRID));
	}

	public OMVRBTreeRIDSet(final ODatabaseRecord iDatabase, String iClusterName) {
		super(new OMVRBTreeRIDProvider(null, iClusterName));
		((OMVRBTreeRIDProvider) dataProvider).setTree(this);
	}

	public void onAfterTxCommit() {
		final Set<ORID> nodesInMemory = getAllNodesInCache();

		if (nodesInMemory.isEmpty())
			return;

		// FIX THE CACHE CONTENT WITH FINAL RECORD-IDS
		final Set<ORID> keys = new HashSet<ORID>(nodesInMemory);
		OMVRBTreeEntryPersistent<ORecordId, ORecordId> entry;
		for (ORID rid : keys) {
			if (rid.getClusterPosition() < -1) {
				// FIX IT IN CACHE
				entry = (OMVRBTreeEntryPersistent<ORecordId, ORecordId>) searchNodeInCache(rid);

				// OVERWRITE IT WITH THE NEW RID
				removeNodeFromCache(rid);
				addNodeInCache(entry);
			}
		}
	}
}
