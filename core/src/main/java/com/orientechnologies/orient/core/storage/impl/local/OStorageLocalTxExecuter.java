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
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.core.tx.OTransactionEntry;
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

	protected long createRecord(final int iRequesterId, final int iTxId, final OCluster iClusterSegment, final byte[] iContent,
			final byte iRecordType) throws IOException {
		long recordPosition = -1;

		try {
			// CREATE DATA SEGMENT. IF TX FAILS AT THIS POINT UN-REFERENCED DATA WILL REMAIN UNTIL NEXT DEFRAG
			final int dataSegment = storage.getDataSegmentForRecord(iClusterSegment, iContent);
			ODataLocal data = storage.getDataSegment(dataSegment);
			final long dataOffset = data.addRecord(-1, -1, iContent);

			// REFERENCE IN THE CLUSTER THE DATA JUST CREATED. IF TX FAILS AT THIS POINT ???
			// TODO
			recordPosition = iClusterSegment.addPhysicalPosition(dataSegment, dataOffset, iRecordType);

			// SAVE INTO THE LOG THE POSITION OF THE RECORD JUST CREATED. IF TX FAILS AT THIS POINT ???
			// TODO
			txSegment.addLog(OTxSegment.OPERATION_CREATE, iRequesterId, iTxId, iClusterSegment.getId(), recordPosition, dataOffset);

		} catch (IOException e) {

			OLogManager.instance().error(this, "Error on creating entry in log segment: " + iClusterSegment, e,
					OTransactionException.class);
		}

		return recordPosition;
	}

	protected int updateRecord(final int iRequesterId, final int iTxId, final OCluster iClusterSegment, final long iPosition,
			final byte[] iContent, final int iVersion, final byte iRecordType) {
		try {
			// READ CURRENT RECORD CONTENT
			final ORawBuffer buffer = storage.readRecord(iRequesterId, iClusterSegment, iPosition, true);

			final long dataOffset;
			if (buffer.buffer != null) {
				// CREATE A COPY OF IT IN DATASEGMENT. IF TX FAILS AT THIS POINT UN-REFERENCED DATA WILL REMAIN UNTIL NEXT DEFRAG
				dataOffset = storage.getDataSegment(storage.getDataSegmentForRecord(iClusterSegment, buffer.buffer)).addRecord(-1, -1,
						buffer.buffer);
			} else
				// NO DATA
				dataOffset = -1;

			// SAVE INTO THE LOG THE POSITION OF THE RECORD JUST DELETED. IF TX FAILS AT THIS POINT AS ABOVE
			txSegment.addLog(OTxSegment.OPERATION_UPDATE, iRequesterId, iTxId, iClusterSegment.getId(), iPosition, dataOffset);

			// UPDATE THE RECORD FOR REAL. IF TX FAILS AT THIS POINT CAN BE RECOVERED THANKS TO THE TX-LOG
			return storage.updateRecord(iRequesterId, iClusterSegment, iPosition, iContent, iVersion, iRecordType);
		} catch (IOException e) {

			OLogManager.instance().error(this, "Error on updating entry #" + iPosition + " in log segment: " + iClusterSegment, e,
					OTransactionException.class);
		}
		return -1;
	}

	protected void deleteRecord(final int iRequesterId, final int iTxId, final OCluster iClusterSegment, final long iPosition,
			final int iVersion) {
		try {
			// GET THE PPOS OF THE RECORD
			OPhysicalPosition ppos = iClusterSegment.getPhysicalPosition(iPosition, new OPhysicalPosition());

			// SAVE INTO THE LOG THE POSITION OF THE RECORD JUST DELETED
			txSegment.addLog(OTxSegment.OPERATION_DELETE, iRequesterId, iTxId, iClusterSegment.getId(), iPosition, ppos.dataPosition);

			// DELETE THE RECORD BUT LEAVING THE PPOS INTACT BUT THE VERSION = -1 TO RECOGNIZE THAT THE ENTRY HAS BEEN DELETED. IF TX
			// FAILS AT THIS POINT CAN BE RECOVERED THANKS TO THE TX-LOG
			iClusterSegment.removePhysicalPosition(iPosition, ppos);
		} catch (IOException e) {

			OLogManager.instance().error(this, "Error on deleting entry #" + iPosition + " in log segment: " + iClusterSegment, e,
					OTransactionException.class);
		}
	}

	public OTxSegment getTxSegment() {
		return txSegment;
	}

	protected void commitAllPendingRecords(final int iRequesterId, final OTransaction iTx) throws IOException {
		// COPY ALL THE ENTRIES IN SEPARATE COLLECTION SINCE DURING THE COMMIT PHASE SOME NEW ENTRIES COULD BE CREATED AND
		// CONCURRENT-EXCEPTION MAY OCCURS
		final List<OTransactionEntry> allEntries = new ArrayList<OTransactionEntry>();
		final List<OTransactionEntry> tmpEntries = new ArrayList<OTransactionEntry>();

		while (iTx.getEntries().iterator().hasNext()) {
			for (OTransactionEntry txEntry : iTx.getEntries())
				tmpEntries.add(txEntry);

			iTx.clearEntries();

			for (OTransactionEntry txEntry : tmpEntries)
				// COMMIT ALL THE SINGLE ENTRIES ONE BY ONE
				commitEntry(iRequesterId, iTx.getId(), txEntry);

			allEntries.addAll(tmpEntries);
			tmpEntries.clear();
		}

		// CLEAR ALL TEMPORARY RECORDS
		txSegment.clearLogEntries(iRequesterId, iTx.getId());

		// UPDATE THE CACHE ONLY IF THE ITERATOR ALLOWS IT
		OTransactionAbstract.updateCacheFromEntries(storage, iTx, allEntries);

		allEntries.clear();
	}

	protected void rollback() {
		// TODO
	}

	private void commitEntry(final int iRequesterId, final int iTxId, final OTransactionEntry txEntry) throws IOException {

		if (txEntry.status != OTransactionEntry.DELETED && !txEntry.getRecord().isDirty())
			return;

		final ORecordId rid = (ORecordId) txEntry.getRecord().getIdentity();

		final OCluster cluster = txEntry.clusterName != null ? storage.getClusterByName(txEntry.clusterName) : storage
				.getClusterById(rid.clusterId);

		if (!(cluster instanceof OClusterLocal))
			// ONLY LOCAL CLUSTER ARE INVOLVED IN TX
			return;

		if (txEntry.getRecord() instanceof OTxListener)
			((OTxListener) txEntry.getRecord()).onEvent(txEntry, OTxListener.EVENT.BEFORE_COMMIT);

		switch (txEntry.status) {
		case OTransactionEntry.LOADED:
			break;

		case OTransactionEntry.CREATED:
			// CHECK 2 TIMES TO ASSURE THAT IT'S A CREATE OR AN UPDATE BASED ON RECURSIVE TO-STREAM METHOD
			final byte[] stream = txEntry.getRecord().toStream();

			if (rid.isNew()) {
				rid.clusterPosition = createRecord(iRequesterId, iTxId, cluster, stream, txEntry.getRecord().getRecordType());
				rid.clusterId = cluster.getId();
			} else {
				txEntry.getRecord().setVersion(
						updateRecord(iRequesterId, iTxId, cluster, rid.clusterPosition, stream, txEntry.getRecord().getVersion(), txEntry
								.getRecord().getRecordType()));
			}
			break;

		case OTransactionEntry.UPDATED:
			txEntry.getRecord().setVersion(
					updateRecord(iRequesterId, iTxId, cluster, rid.clusterPosition, txEntry.getRecord().toStream(), txEntry.getRecord()
							.getVersion(), txEntry.getRecord().getRecordType()));
			break;

		case OTransactionEntry.DELETED:
			deleteRecord(iRequesterId, iTxId, cluster, rid.clusterPosition, txEntry.getRecord().getVersion());
			break;
		}

		txEntry.getRecord().unsetDirty();

		if (txEntry.getRecord() instanceof OTxListener)
			((OTxListener) txEntry.getRecord()).onEvent(txEntry, OTxListener.EVENT.AFTER_COMMIT);
	}
}
