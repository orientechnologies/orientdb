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
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
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
			// CREATE DATA SEGMENT. IF TX FAILS AT THIS POINT UN-REFERENCED DATA WILL REMAIN UNTIL NEXT DEFRAG
			final int dataSegment = storage.getDataSegmentForRecord(iClusterSegment, iContent);
			final ODataLocal data = storage.getDataSegment(dataSegment);

			// REFERENCE IN THE CLUSTER THE DATA JUST CREATED. IF TX FAILS AT THIS POINT AN EMPTY ENTRY IS KEPT UNTIL DEFRAG
			iRid.clusterPosition = iClusterSegment.addPhysicalPosition(-1, -1, iRecordType);

			final long dataOffset = data.addRecord(iRid, iContent);

			// UPDATE THE POSITION IN CLUSTER WITH THE POSITION OF RECORD IN DATA
			iClusterSegment.setPhysicalPosition(iRid.clusterPosition, dataSegment, dataOffset, iRecordType, 0);

			// SAVE INTO THE LOG THE POSITION OF THE RECORD JUST CREATED. IF TX FAILS AT THIS POINT A GHOST RECORD IS CREATED UNTIL DEFRAG
			txSegment.addLog(OTxSegment.OPERATION_CREATE, iTxId, iRid.clusterId, iRid.clusterPosition, dataSegment, dataOffset, 0);

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
			final OPhysicalPosition ppos = iClusterSegment.getPhysicalPosition(iRid.clusterPosition, new OPhysicalPosition());
			if (ppos == null)
				// DELETED
				throw new OTransactionException("Can't retrieve the updated record #" + iRid);

			// MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
			if (iVersion > -1 && ppos.version != iVersion)
				throw new OConcurrentModificationException(
						"Can't update the record "
								+ iRid
								+ " because the version is not the latest one. Probably you are updating an old record or it has been modified by another user (db=v"
								+ ppos.version + " your=v" + iVersion + ")");

			final ODataLocal dataSegment = storage.getDataSegment(storage.getDataSegmentForRecord(iClusterSegment, iContent));

			// STORE THE NEW CONTENT
			final long dataOffset;
			if (iContent != null) {
				// IF TX FAILS AT THIS POINT UN-REFERENCED DATA WILL REMAIN UNTIL NEXT DEFRAG
				dataOffset = dataSegment.addRecord(iRid, iContent);
				iClusterSegment.setPhysicalPosition(iRid.clusterPosition, dataSegment.getId(), dataOffset, iRecordType, ++ppos.version);
			} else
				// NO DATA
				dataOffset = -1;

			dataSegment.updateRid(ppos.dataPosition, ORecordId.EMPTY_RECORD_ID);

			// SAVE INTO THE LOG THE POSITION OF THE OLD RECORD JUST DELETED. IF TX FAILS AT THIS POINT AS ABOVE
			txSegment.addLog(OTxSegment.OPERATION_UPDATE, iTxId, iRid.clusterId, iRid.clusterPosition, ppos.dataSegment,
					ppos.dataPosition, ppos.version - 1);

			return ppos.version;

		} catch (IOException e) {

			OLogManager.instance().error(this, "Error on updating entry #" + iRid + " in log segment: " + iClusterSegment, e,
					OTransactionException.class);
		}
		return -1;
	}

	protected void deleteRecord(final int iTxId, final OCluster iClusterSegment, final long iPosition, final int iVersion) {
		try {
			// GET THE PPOS OF THE RECORD
			final OPhysicalPosition ppos = iClusterSegment.getPhysicalPosition(iPosition, new OPhysicalPosition());

			if (!storage.checkForRecordValidity(ppos))
				// ALREADY DELETED
				return;

			// MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
			if (iVersion > -1 && ppos.version != iVersion)
				throw new OConcurrentModificationException(
						"Can't delete the record #"
								+ iClusterSegment.getId()
								+ ":"
								+ iPosition
								+ " because the version is not the latest one. Probably you are deleting an old record or it has been modified by another user (db=v"
								+ ppos.version + " your=v" + iVersion + ")");

			// SAVE INTO THE LOG THE POSITION OF THE RECORD JUST DELETED
			txSegment.addLog(OTxSegment.OPERATION_DELETE, iTxId, iClusterSegment.getId(), iPosition, ppos.dataSegment, ppos.dataPosition,
					iVersion);

			// DELETE THE RECORD BUT LEAVING THE PPOS INTACT, BUT THE VERSION = -1 TO RECOGNIZE THAT THE ENTRY HAS BEEN DELETED. IF TX
			// FAILS AT THIS POINT CAN BE RECOVERED THANKS TO THE TX-LOG. NO CONCURRENT THREAD CAN REUSE THE HOLE CREATED BECAUSE THE
			// EXCLUSIVE LOCK
			iClusterSegment.removePhysicalPosition(iPosition, ppos);

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
					createRecord(iTx.getId(), cluster, rid, stream, txEntry.getRecord().getRecordType());
				else
					iTx.getDatabase().getStorage().createRecord(rid, stream, txEntry.getRecord().getRecordType(), null);

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

			txEntry.getRecord().setVersion(
					updateRecord(iTx.getId(), cluster, rid, stream, txEntry.getRecord().getVersion(), txEntry.getRecord().getRecordType()));

			iTx.getDatabase().callbackHooks(ORecordHook.TYPE.AFTER_UPDATE, txEntry.getRecord());
			break;
		}

		case OTransactionRecordEntry.DELETED: {
			iTx.getDatabase().callbackHooks(ORecordHook.TYPE.BEFORE_DELETE, txEntry.getRecord());

			deleteRecord(iTx.getId(), cluster, rid.clusterPosition, txEntry.getRecord().getVersion());

			iTx.getDatabase().callbackHooks(ORecordHook.TYPE.AFTER_DELETE, txEntry.getRecord());
		}
			break;
		}

		txEntry.getRecord().unsetDirty();

		if (txEntry.getRecord() instanceof OTxListener)
			((OTxListener) txEntry.getRecord()).onEvent(txEntry, OTxListener.EVENT.AFTER_COMMIT);
	}
}
