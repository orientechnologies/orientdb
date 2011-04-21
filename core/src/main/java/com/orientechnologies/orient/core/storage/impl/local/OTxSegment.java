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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.config.OStorageTxConfiguration;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;

/**
 * Handle the records that wait to be committed. This class is not synchronized because the caller is responsible of it.<br/>
 * Record structure:<br/>
 * <br/>
 * +--------+--------+---------+---------+----------------+-------------+<br/>
 * | STATUS | OPERAT | TX ID . | CLUSTER | CLUSTER OFFSET | DATA OFFSET |<br/>
 * | 1 byte | 1 byte | 4 bytes | 2 bytes | 8 bytes ...... | 8 bytes ... |<br/>
 * +--------+--------+---------+---------+----------------+-------------+<br/>
 * = 24 bytes<br/>
 */
public class OTxSegment extends OSingleFileSegment {
	public static final byte	STATUS_FREE				= 0;
	public static final byte	STATUS_COMMITTING	= 1;

	public static final byte	OPERATION_CREATE	= 0;
	public static final byte	OPERATION_DELETE	= 1;
	public static final byte	OPERATION_UPDATE	= 2;

	private static final int	DEF_START_SIZE		= 262144;
	private static final int	RECORD_SIZE				= 26;

	public OTxSegment(final OStorageLocal iStorage, final OStorageTxConfiguration iConfig) throws IOException {
		super(iStorage, iConfig);
	}

	/**
	 * Starts the recovery of pending transactions, if any
	 */
	@Override
	public boolean open() throws IOException {
		acquireExclusiveLock();

		try {
			// IGNORE IF IT'S SOFTLY CLOSED
			super.open();

			// CHECK FOR PENDING TRANSACTION ENTRIES TO RECOVER
			int size = (file.getFilledUpTo() / RECORD_SIZE);

			if (size == 0)
				return true;

			recoverTransactions();

			return true;

		} finally {
			releaseExclusiveLock();
		}
	}

	@Override
	public void create(final int iStartSize) throws IOException {
		super.create(iStartSize > -1 ? iStartSize : DEF_START_SIZE);
	}

	/**
	 * Append a log entry
	 * 
	 * @param iReqId
	 *          The id of requester
	 * @param iTxId
	 *          The id of transaction
	 * 
	 * @throws IOException
	 */
	public void addLog(final byte iOperation, final int iTxId, final int iClusterId, final long iPosition, final long iDataOffset)
			throws IOException {
		acquireExclusiveLock();
		try {
			int offset = file.allocateSpace(RECORD_SIZE);

			file.writeByte(offset, STATUS_COMMITTING);
			offset += OConstants.SIZE_BYTE;

			file.writeByte(offset, iOperation);
			offset += OConstants.SIZE_BYTE;

			file.writeInt(offset, iTxId);
			offset += OConstants.SIZE_INT;

			file.writeShort(offset, (short) iClusterId);
			offset += OConstants.SIZE_SHORT;

			file.writeLong(offset, iPosition);
			offset += OConstants.SIZE_LONG;

			file.writeLong(offset, iDataOffset);

			synchRecord();

		} finally {
			releaseExclusiveLock();
		}
	}

	/**
	 * Clear all the transaction entries by setting the status to STATUS_CLEARING
	 * 
	 * @param iReqId
	 *          The id of requester
	 * @param iTxId
	 *          The id of transaction
	 * 
	 * @throws IOException
	 */
	public void clearLogEntries(final int iTxId) throws IOException {
		acquireExclusiveLock();
		try {
			int size = (file.getFilledUpTo() / RECORD_SIZE);
			byte status;
			int txId;
			int offset;

			int recordFreed = 0;

			for (int i = 0; i < size; ++i) {
				// READ THE STATUS
				offset = i * RECORD_SIZE;

				status = file.readByte(offset);

				if (status == STATUS_COMMITTING) {
					// READ THE REQ-ID
					offset += OConstants.SIZE_BYTE + OConstants.SIZE_BYTE;
					txId = file.readInt(offset);

					if (txId == iTxId) {
						// CURRENT REQUESTER & TX: CLEAR THE ENTRY BY WRITING THE "FREE" STATUS
						file.writeByte(i * RECORD_SIZE, STATUS_FREE);
						recordFreed++;
					}
				}
			}

			// SHRINK THE FILE TO THE LAST GOOD POSITION. USE THE COUNTER OF PREVIOUS CYCLE TO DETERMINE THE NUMBER OF RECORDS FREED FOR
			// THIS TX
			int lastRecord = size - 1;

			for (int i = size - 1; i > -1 && recordFreed > 0; --i) {
				offset = i * RECORD_SIZE;

				status = file.readByte(offset);
				offset += OConstants.SIZE_BYTE;
				offset += OConstants.SIZE_BYTE;

				txId = file.readShort(offset);

				if (txId != iTxId)
					// NO MY TX, EXIT
					break;

				lastRecord = i;
				recordFreed--;
			}

			if (lastRecord > -1)
				file.shrink(lastRecord * RECORD_SIZE);

			synchTx();

		} finally {
			releaseExclusiveLock();
		}
	}

	public int getTotalLogCount() {
		acquireSharedLock();
		try {
			return (file.getFilledUpTo() / RECORD_SIZE);
		} finally {
			releaseSharedLock();
		}
	}

	private void recoverTransactions() throws IOException {
		OLogManager.instance().debug(
				this,
				"Started the recovering of pending transactions after a brute shutdown. Found " + getTotalLogCount()
						+ " entry logs. Scanning...");

		int recoveredTxs = 0;
		int recoveredRecords = 0;
		int rec;

		Map<Integer, Integer> txToRecover = scanForTransactionsToRecover();
		for (Entry<Integer, Integer> entry : txToRecover.entrySet()) {
			rec = recoverTransaction(entry.getKey(), entry.getValue());

			if (rec > 0) {
				recoveredTxs++;
				recoveredRecords += rec;
			}
		}

		// EMPTY THE FILE
		file.shrink(0);

		if (recoveredRecords > 0) {
			OLogManager.instance().info(this, "Recovering successfully completed:");
			OLogManager.instance().info(this, "- Recovered Tx.....: " + recoveredTxs);
			OLogManager.instance().info(this, "- Recovered Records: " + recoveredRecords);
		} else
			OLogManager.instance().debug(this, "Recovering successfully completed: no pending tx records found.");

	}

	private Map<Integer, Integer> scanForTransactionsToRecover() throws IOException {
		byte status;
		int reqId;
		int txId;

		int offset;

		// SCAN ALL THE FILE SEARCHING FOR THE TRANSACTIONS TO RECOVER
		Map<Integer, Integer> txToRecover = new HashMap<Integer, Integer>();
		Map<Integer, Integer> txToNotRecover = new HashMap<Integer, Integer>();

		int size = (file.getFilledUpTo() / RECORD_SIZE);
		for (int i = 0; i < size; ++i) {
			offset = i * RECORD_SIZE;

			status = file.readByte(offset);
			offset += OConstants.SIZE_BYTE;
			offset += OConstants.SIZE_BYTE;

			reqId = file.readShort(offset);
			offset += OConstants.SIZE_SHORT;

			txId = file.readShort(offset);

			switch (status) {
			case STATUS_FREE:
				// NOT RECOVER IT SINCE IF FIND AT LEAST ONE "FREE" STATUS MEANS THAT ALL THE LOGS WAS COMMITTED BUT THE USER DIDN'T
				// RECEIVED THE ACK
				txToNotRecover.put(reqId, txId);
				break;

			case STATUS_COMMITTING:
				// TO RECOVER UNLESS THE REQ/TX IS IN THE MAP txToNotRecover
				txToRecover.put(reqId, txId);
				break;
			}
		}

		// FILTER THE TX MAP TO RECOVER BY REMOVING THE TX WITH AT LEAST ONE "FREE" STATUS
		Entry<Integer, Integer> entry;
		for (Iterator<Entry<Integer, Integer>> it = txToRecover.entrySet().iterator(); it.hasNext();) {
			entry = it.next();
			if (txToNotRecover.containsKey(entry.getKey()) && txToNotRecover.get(entry.getKey()).equals(entry.getValue()))
				// NO TO RECOVER SINCE AT LEAST ONE "FREE" STATUS MEANS THAT ALL THE LOGS WAS COMMITTED BUT THE USER DIDN'T
				// RECEIVED THE ACK
				it.remove();
		}

		return txToRecover;
	}

	/**
	 * Recover a transaction.
	 * 
	 * @param iReqId
	 * @param iTxId
	 * @return Number of records recovered
	 * 
	 * @throws IOException
	 */
	private int recoverTransaction(int iReqId, int iTxId) throws IOException {
		byte status;
		byte operation;
		int reqId;
		int txId;
		long oldDataOffset;

		int offset;
		OPhysicalPosition ppos = new OPhysicalPosition();

		int size = (file.getFilledUpTo() / RECORD_SIZE);
		int recordsRecovered = 0;
		final ORecordId rid = new ORecordId();

		for (int i = 0; i < size; ++i) {
			offset = i * RECORD_SIZE;

			status = file.readByte(offset);
			offset += OConstants.SIZE_BYTE;

			if (status != STATUS_FREE) {

				// DIRTY TX LOG ENTRY
				operation = file.readByte(offset);
				offset += OConstants.SIZE_BYTE;

				reqId = file.readShort(offset);

				if (reqId == iReqId) {
					// REQ ID FOUND
					offset += OConstants.SIZE_SHORT;
					txId = file.readInt(offset);

					if (txId == iTxId) {
						// TX ID FOUND
						offset += OConstants.SIZE_INT;

						rid.clusterId = file.readShort(offset);
						offset += OConstants.SIZE_SHORT;

						rid.clusterPosition = file.readLong(offset);
						offset += OConstants.SIZE_LONG;

						oldDataOffset = file.readLong(offset);

						recoverTransactionEntry(status, operation, reqId, txId, rid, oldDataOffset, ppos);
						recordsRecovered++;

						// CLEAR THE ENTRY BY WRITING '0'
						file.writeByte(i * RECORD_SIZE + OConstants.SIZE_SHORT + OConstants.SIZE_INT, STATUS_FREE);
					}
				}
			}
		}
		return recordsRecovered;
	}

	private void recoverTransactionEntry(byte status, byte operation, int reqId, int txId, final ORecordId iRid, long oldDataOffset,
			OPhysicalPosition ppos) throws IOException {

		final OCluster cluster = storage.getClusterById(iRid.clusterId);

		if (!(cluster instanceof OClusterLocal))
			return;

		final OClusterLocal logCluster = (OClusterLocal) cluster;

		OLogManager.instance().info(this,
				"Recovering tx <%d> by req <%d>. Operation <%d> was in status <%d> on record %s in data space %d...", txId, reqId,
				operation, status, iRid, oldDataOffset);

		switch (operation) {
		case OPERATION_CREATE:
			// JUST DELETE THE RECORD
			storage.deleteRecord(iRid, -1);
			break;

		case OPERATION_UPDATE:
			// RETRIEVE THE OLD RECORD
			// final int recSize = storage.getDataSegment(ppos.dataSegment).getRecordSize(oldDataOffset);

			// RETRIEVE THE CURRENT PPOS
			cluster.getPhysicalPosition(iRid.clusterPosition, ppos);

			long newPosition = ppos.dataPosition;
			int newSize = ppos.recordSize;

			// REPLACE THE POSITION OF THE OLD RECORD
			ppos.dataPosition = oldDataOffset;

			// UPDATE THE PPOS WITH THE COORDS OF THE OLD RECORD
			storage.getClusterById(iRid.clusterId).setPhysicalPosition(iRid.clusterPosition, ppos.dataSegment, oldDataOffset, ppos.type);

			// CREATE A HOLE
			storage.getDataSegment(ppos.dataSegment).createHole(newPosition, newSize);
			break;

		case OPERATION_DELETE:
			// GET THE PPOS
			cluster.getPhysicalPosition(iRid.clusterPosition, ppos);

			// SAVE THE PPOS WITH THE VERSION TO 0 (VALID IF >-1)
			cluster.updateVersion(iRid.clusterPosition, 0);

			// REMOVE THE HOLE
			logCluster.removeHole(iRid.clusterPosition);
			break;
		}
	}

	private void synchRecord() {
		if (((OStorageTxConfiguration) config).isSynchRecord())
			file.synch();
	}

	private void synchTx() {
		if (((OStorageTxConfiguration) config).isSynchTx())
			file.synch();
	}
}
