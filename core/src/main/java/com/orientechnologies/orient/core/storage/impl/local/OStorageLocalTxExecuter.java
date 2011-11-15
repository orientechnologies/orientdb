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
package com.orientechnologies.orient.core.storage.impl.local;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OStorageTxConfiguration;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.core.tx.OTransactionRecordEntry;
import com.orientechnologies.orient.core.tx.OTxListener;

public class OStorageLocalTxExecuter {
	private final OStorageLocal	storage;
	private final OTxSegment		txSegment;

	public OStorageLocalTxExecuter(final OStorageLocal iStorage, final OStorageTxConfiguration iConfig) throws IOException {
		storage = iStorage;

		iConfig.path = OStorageVariableParser.DB_PATH_VARIABLE + "/txlog.otx";

		txSegment = new OTxSegment(storage, iStorage.getConfiguration().txSegment);
	}

	public void open() throws IOException {
		txSegment.open();
	}

	public void create() throws IOException {
		txSegment.create(1000000);
	}

	public void close() throws IOException {
		txSegment.close();
	}

	protected long createRecord(final int iTxId, final OCluster iClusterSegment, final ORecordId iRid, final byte[] iContent,
			final byte iRecordType) throws IOException {
		iRid.clusterPosition = -1;

		try {
			// SAVE INTO THE LOG THE POSITION OF THE RECORD JUST CREATED. IF TX FAILS AT THIS POINT A GHOST RECORD IS CREATED UNTIL DEFRAG
			txSegment.addLog(OTxSegment.OPERATION_CREATE, iTxId, iRid.clusterId, iRid.clusterPosition, iRecordType, 0, null);

			return storage.createRecord(iClusterSegment, iContent, iRecordType);

		} catch (IOException e) {

			OLogManager.instance().error(this, "Error on creating entry in log segment: " + iClusterSegment, e,
					OTransactionException.class);
		}

		return iRid.clusterPosition;
	}

	/**
	 * Stores the new content in a new position, then saves in the log the coords of the new position. At free time the
	 * 
	 * @param iTxId
	 * @param iClusterSegment
	 * @param iRid
	 * @param iContent
	 * @param iVersion
	 * @param iRecordType
	 * @return
	 */

	protected int updateRecord(final int iTxId, final OCluster iClusterSegment, final ORecordId iRid, final byte[] iContent,
			final int iVersion, final byte iRecordType) {
		try {
			// READ CURRENT RECORD CONTENT
			final ORawBuffer buffer = storage.readRecord(iClusterSegment, iRid, false);

			// SAVE INTO THE LOG THE POSITION OF THE OLD RECORD JUST DELETED. IF TX FAILS AT THIS POINT AS ABOVE
			txSegment.addLog(OTxSegment.OPERATION_UPDATE, iTxId, iRid.clusterId, iRid.clusterPosition, iRecordType, buffer.version - 1,
					buffer.buffer);

			return storage.updateRecord(iClusterSegment, iRid, iContent, iVersion, iRecordType);

		} catch (IOException e) {

			OLogManager.instance().error(this, "Error on updating entry #" + iRid + " in log segment: " + iClusterSegment, e,
					OTransactionException.class);
		}
		return -1;
	}

	protected void deleteRecord(final int iTxId, final OCluster iClusterSegment, final long iPosition, final int iVersion) {
		try {
			final ORecordId rid = new ORecordId(iClusterSegment.getId(), iPosition);

			// READ CURRENT RECORD CONTENT
			final ORawBuffer buffer = storage.readRecord(iClusterSegment, rid, false);

			// SAVE INTO THE LOG THE OLD RECORD
			txSegment.addLog(OTxSegment.OPERATION_DELETE, iTxId, iClusterSegment.getId(), iPosition, buffer.recordType, buffer.version,
					buffer.buffer);

			storage.deleteRecord(iClusterSegment, rid, iVersion);

		} catch (IOException e) {

			OLogManager.instance().error(this, "Error on deleting entry #" + iPosition + " in log segment: " + iClusterSegment, e,
					OTransactionException.class);
		}
	}

	public OTxSegment getTxSegment() {
		return txSegment;
	}

	public void commitAllPendingRecords(final OTransaction iTx) throws IOException {
		// COPY ALL THE ENTRIES IN SEPARATE COLLECTION SINCE DURING THE COMMIT PHASE SOME NEW ENTRIES COULD BE CREATED AND
		// CONCURRENT-EXCEPTION MAY OCCURS
		final List<OTransactionRecordEntry> tmpEntries = new ArrayList<OTransactionRecordEntry>();

		while (iTx.getCurrentRecordEntries().iterator().hasNext()) {
			for (OTransactionRecordEntry txEntry : iTx.getCurrentRecordEntries())
				tmpEntries.add(txEntry);

			iTx.clearRecordEntries();

			if (!tmpEntries.isEmpty()) {
				for (OTransactionRecordEntry txEntry : tmpEntries)
					// COMMIT ALL THE SINGLE ENTRIES ONE BY ONE
					commitEntry(iTx, txEntry, iTx.isUsingLog());
			}
		}

		// UPDATE THE CACHE ONLY IF THE ITERATOR ALLOWS IT
		OTransactionAbstract.updateCacheFromEntries(storage, iTx, iTx.getAllRecordEntries(), true);
	}

	public void clearLogEntries(final OTransaction iTx) throws IOException {
		// CLEAR ALL TEMPORARY RECORDS
		txSegment.clearLogEntries(iTx.getId());
	}

	private void commitEntry(final OTransaction iTx, final OTransactionRecordEntry txEntry, final boolean iUseLog) throws IOException {

		if (txEntry.status != OTransactionRecordEntry.DELETED && !txEntry.getRecord().isDirty())
			return;

		final ORecordId rid = (ORecordId) txEntry.getRecord().getIdentity();

		final OCluster cluster = txEntry.clusterName != null ? storage.getClusterByName(txEntry.clusterName) : storage
				.getClusterById(rid.clusterId);

		if (cluster.getName().equals(OStorage.CLUSTER_INDEX_NAME))
			// AVOID TO COMMIT INDEX STUFF
			return;

		if (!(cluster instanceof OClusterLocal))
			// ONLY LOCAL CLUSTER ARE INVOLVED IN TX
			return;

		if (txEntry.getRecord() instanceof OTxListener)
			((OTxListener) txEntry.getRecord()).onEvent(txEntry, OTxListener.EVENT.BEFORE_COMMIT);

		switch (txEntry.status) {
		case OTransactionRecordEntry.LOADED:
			break;

		case OTransactionRecordEntry.CREATED: {
			// CHECK 2 TIMES TO ASSURE THAT IT'S A CREATE OR AN UPDATE BASED ON RECURSIVE TO-STREAM METHOD
			byte[] stream = txEntry.getRecord().toStream();

			if (rid.isNew()) {
				if (iTx.getDatabase().callbackHooks(ORecordHook.TYPE.BEFORE_CREATE, txEntry.getRecord()))
					// RECORD CHANGED: RE-STREAM IT
					stream = txEntry.getRecord().toStream();

				rid.clusterId = cluster.getId();

				if (iUseLog)
					rid.clusterPosition = createRecord(iTx.getId(), cluster, rid, stream, txEntry.getRecord().getRecordType());
				else
					rid.clusterPosition = iTx.getDatabase().getStorage().createRecord(rid, stream, txEntry.getRecord().getRecordType(), null);

				iTx.getDatabase().callbackHooks(ORecordHook.TYPE.AFTER_CREATE, txEntry.getRecord());
			} else {
				if (iUseLog)
					txEntry.getRecord()
							.setVersion(
									updateRecord(iTx.getId(), cluster, rid, stream, txEntry.getRecord().getVersion(), txEntry.getRecord()
											.getRecordType()));
				else
					txEntry.getRecord().setVersion(
							iTx.getDatabase().getStorage()
									.updateRecord(rid, stream, txEntry.getRecord().getVersion(), txEntry.getRecord().getRecordType(), null));
			}
			break;
		}

		case OTransactionRecordEntry.UPDATED: {
			byte[] stream = txEntry.getRecord().toStream();

			if (iTx.getDatabase().callbackHooks(ORecordHook.TYPE.BEFORE_UPDATE, txEntry.getRecord()))
				// RECORD CHANGED: RE-STREAM IT
				stream = txEntry.getRecord().toStream();

			if (iUseLog)
				txEntry.getRecord().setVersion(
						updateRecord(iTx.getId(), cluster, rid, stream, txEntry.getRecord().getVersion(), txEntry.getRecord().getRecordType()));
			else
				iTx.getDatabase().getStorage()
						.updateRecord(rid, stream, txEntry.getRecord().getVersion(), txEntry.getRecord().getRecordType(), null);

			iTx.getDatabase().callbackHooks(ORecordHook.TYPE.AFTER_UPDATE, txEntry.getRecord());
			break;
		}

		case OTransactionRecordEntry.DELETED: {
			iTx.getDatabase().callbackHooks(ORecordHook.TYPE.BEFORE_DELETE, txEntry.getRecord());

			if (iUseLog)
				deleteRecord(iTx.getId(), cluster, rid.clusterPosition, txEntry.getRecord().getVersion());
			else
				iTx.getDatabase().getStorage().deleteRecord(rid, txEntry.getRecord().getVersion(), null);

			iTx.getDatabase().callbackHooks(ORecordHook.TYPE.AFTER_DELETE, txEntry.getRecord());
		}
			break;
		}

		txEntry.getRecord().unsetDirty();

		if (txEntry.getRecord() instanceof OTxListener)
			((OTxListener) txEntry.getRecord()).onEvent(txEntry, OTxListener.EVENT.AFTER_COMMIT);
	}
}
