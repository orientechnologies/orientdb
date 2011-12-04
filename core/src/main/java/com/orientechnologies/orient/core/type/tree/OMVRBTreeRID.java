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

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeProvider;
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeRIDProvider;

/**
 * Persistent MVRB-Tree Set implementation.
 * 
 */
public class OMVRBTreeRID extends OMVRBTreePersistent<OIdentifiable, OIdentifiable> {
	private static final long	serialVersionUID		= 1L;

	protected int							maxUpdatesBeforeSave;
	protected int							updates							= 0;
	protected boolean					transactionRunning	= false;

	public OMVRBTreeRID() {
		this(new OMVRBTreeRIDProvider(null, ODatabaseRecordThreadLocal.INSTANCE.get().getDefaultClusterId()));
	}

	public OMVRBTreeRID(final ORID iRID) {
		this(new OMVRBTreeRIDProvider(null, iRID.getClusterId(), iRID));
		load();
	}

	public OMVRBTreeRID(final String iClusterName) {
		this(new OMVRBTreeRIDProvider(null, iClusterName));
	}

	public OMVRBTreeRID(final OMVRBTreeProvider<OIdentifiable, OIdentifiable> iProvider) {
		super(iProvider);
		((OMVRBTreeRIDProvider) dataProvider).setTree(this);
	}

	/**
	 * Do nothing since all the changes will be committed expressly at lazySave() time or on closing.
	 */
	@Override
	public synchronized int commitChanges() {
		if (transactionRunning || maxUpdatesBeforeSave == 0 || (maxUpdatesBeforeSave > 0 && ++updates >= maxUpdatesBeforeSave)) {
			updates = 0;
			return lazySave();
		}
		return 0;
	}

	@Override
	public void clear() {
		super.clear();
		lazySave();
	}

	public int lazySave() {
		return super.commitChanges();
	}

	/**
	 * Returns the maximum updates to save the map persistently.
	 * 
	 * @return 0 means no automatic save, 1 means non-lazy map (save each operation) and > 1 is lazy.
	 */
	public int getMaxUpdatesBeforeSave() {
		return maxUpdatesBeforeSave;
	}

	/**
	 * Sets the maximum updates to save the map persistently.
	 * 
	 * @param iValue
	 *          0 means no automatic save, 1 means non-lazy map (save each operation) and > 1 is lazy.
	 */
	public void setMaxUpdatesBeforeSave(final int iValue) {
		this.maxUpdatesBeforeSave = iValue;
	}

	@Override
	protected void config() {
		super.config();
		maxUpdatesBeforeSave = OGlobalConfiguration.MVRBTREE_LAZY_UPDATES.getValueAsInteger();
	}

	/**
	 * Change the transaction running mode.
	 * 
	 * @param iTxRunning
	 *          true if a transaction is running, otherwise false
	 */
	public void setRunningTransaction(final boolean iTxRunning) {
		transactionRunning = iTxRunning;

		if (iTxRunning) {
			// ASSURE ALL PENDING CHANGES ARE COMMITTED BEFORE TO START A TX
			updates = 0;
			lazySave();
		}
	}

	public void onAfterTxCommit() {
		final Set<ORID> nodesInMemory = getAllNodesInCache();

		if (nodesInMemory.isEmpty())
			return;

		// FIX THE CACHE CONTENT WITH FINAL RECORD-IDS
		final Set<ORID> keys = new HashSet<ORID>(nodesInMemory);
		OMVRBTreeEntryPersistent<OIdentifiable, OIdentifiable> entry;
		for (ORID rid : keys) {
			if (rid.getClusterPosition() < -1) {
				// FIX IT IN CACHE
				entry = (OMVRBTreeEntryPersistent<OIdentifiable, OIdentifiable>) searchNodeInCache(rid);

				// OVERWRITE IT WITH THE NEW RID
				removeNodeFromCache(rid);
				addNodeInCache(entry);
			}
		}
	}
}
