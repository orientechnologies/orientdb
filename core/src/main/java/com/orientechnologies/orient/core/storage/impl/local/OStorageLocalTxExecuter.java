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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OStorageTxConfiguration;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionEntry;

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
			ODataLocal data = storage.dataSegments[dataSegment];
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

			// CREATE A COPY OF IT IN DATASEGMENT. IF TX FAILS AT THIS POINT UN-REFERENCED DATA WILL REMAIN UNTIL NEXT DEFRAG
			final long dataOffset = storage.getDataSegment(storage.getDataSegmentForRecord(iClusterSegment, buffer.buffer)).addRecord(-1,
					-1, buffer.buffer);

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

	protected void commitAllPendingRecords(final int iRequesterId, final OTransaction<?> iTx) throws IOException {
		// COMMIT ALL THE SINGLE ENTRIES ONE BY ONE
		for (OTransactionEntry<? extends ORecord<?>> txEntry : iTx.getEntries()) {
			commitEntry(iRequesterId, iTx.getId(), txEntry);
		}

		// CLEAR ALL TEMPORARY RECORDS
		txSegment.clearLogEntries(iRequesterId, iTx.getId());

		// UPDATE THE CACHE ONLY IF THE ITERATOR ALLOWS IT
		String rid;
		ORawBuffer cachedBuffer;
		for (OTransactionEntry<? extends ORecord<?>> txEntry : iTx.getEntries()) {
			rid = txEntry.record.getIdentity().toString();

			cachedBuffer = storage.getCache().getRecord(rid);

			if (cachedBuffer != null) {
				// UPDATE CACHE
				cachedBuffer.buffer = txEntry.record.toStream();
				cachedBuffer.version = txEntry.record.getVersion();
				cachedBuffer.recordType = txEntry.record.getRecordType();

			} else if (txEntry.record.isPinned())
				// INSERT NEW ENTRY IN THE CACHE
				storage.getCache().pushRecord(rid,
						new ORawBuffer(txEntry.record.toStream(), txEntry.record.getVersion(), txEntry.record.getRecordType()));
		}
	}

	protected void rollback() {
		// TODO
	}

	private void commitEntry(final int iRequesterId, final int iTxId, final OTransactionEntry<? extends ORecord<?>> txEntry)
			throws IOException {

		ORecordId rid = (ORecordId) txEntry.record.getIdentity();

		final OCluster cluster = txEntry.clusterName != null ? storage.getClusterByName(txEntry.clusterName) : storage
				.getClusterById(rid.clusterId);

		switch (txEntry.status) {
		case OTransactionEntry.LOADED:
			break;

		case OTransactionEntry.CREATED:
			if (!txEntry.record.getIdentity().isValid()) {
				rid.clusterPosition = createRecord(iRequesterId, iTxId, cluster, txEntry.record.toStream(), txEntry.record.getRecordType());
				rid.clusterId = cluster.getId();
			}
			break;

		case OTransactionEntry.UPDATED:
			txEntry.record.setVersion(updateRecord(iRequesterId, iTxId, cluster, rid.clusterPosition, txEntry.record.toStream(),
					txEntry.record.getVersion(), txEntry.record.getRecordType()));
			break;

		case OTransactionEntry.DELETED:
			deleteRecord(iRequesterId, iTxId, cluster, rid.clusterPosition, txEntry.record.getVersion());
			break;
		}
	}
}
