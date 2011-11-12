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
import java.util.HashSet;
import java.util.Set;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.OConstants;
import com.orientechnologies.orient.core.config.OStorageTxConfiguration;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.tx.OTransaction;

/**
 * Handles the records that wait to be committed. This class is not synchronized because the caller is responsible of it.<br/>
 * <br/>
 * Record structure:<br/>
 * <code>
 * +--------+--------+---------+---------+----------------+---------+-------------+---------+<br/>
 * | STATUS | OPERAT | TX ID . | CLUSTER | CLUSTER OFFSET | DATA ID | DATA OFFSET | VERSION |<br/>
 * | 1 byte | 1 byte | 4 bytes | 2 bytes | 8 bytes ...... | 4 bytes | 8 bytes ... | 4 bytes |<br/>
 * +--------+--------|---------+---------+----------------+---------+-------------|---------+<br/>
 * = 32 bytes
 * </code><br/>
 * At commit time all the changes are written in the TX log file with status = STATUS_COMMITTING. Once all records have been
 * written, then the status of all the records is changed in STATUS_FREE. If a transactions has at least a STATUS_FREE means that
 * has been successfully committed. This is the reason why on startup all the pending transactions will be recovered, but those with
 * at least one record with status = STATUS_FREE.
 */
public class OTxSegment extends OSingleFileSegment {
	public static final byte	STATUS_FREE				= 0;
	public static final byte	STATUS_COMMITTING	= 1;

	public static final byte	OPERATION_CREATE	= 0;
	public static final byte	OPERATION_DELETE	= 1;
	public static final byte	OPERATION_UPDATE	= 2;

	private static final int	DEF_START_SIZE		= 262144;
	private static final int	RECORD_SIZE				= 32;

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
	public void addLog(final byte iOperation, final int iTxId, final int iClusterId, final long iPosition, final int iDataId,
			final long iDataOffset, final int iRecordVersion) throws IOException {
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

			file.writeInt(offset, iDataId);
			offset += OConstants.SIZE_INT;

			file.writeLong(offset, iDataOffset);
			offset += OConstants.SIZE_LONG;

			file.writeInt(offset, iRecordVersion);

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
			ORecordId rid = new ORecordId();

			for (int i = 0; i < size; ++i) {
				// READ THE STATUS
				offset = i * RECORD_SIZE;

				status = file.readByte(offset);
				offset += OConstants.SIZE_BYTE;

				if (status == STATUS_COMMITTING) {
					final byte operation = file.readByte(offset);
					offset += OConstants.SIZE_BYTE;

					// READ THE TX-ID
					txId = file.readInt(offset);

					if (txId == iTxId) {
						// TX ID FOUND
						offset += OConstants.SIZE_INT;

						rid.clusterId = file.readShort(offset);
						offset += OConstants.SIZE_SHORT;

						rid.clusterPosition = file.readLong(offset);
						offset += OConstants.SIZE_LONG;

						final int oldDataId = file.readInt(offset);
						offset += OConstants.SIZE_INT;

						final long oldDataOffset = file.readLong(offset);
						offset += OConstants.SIZE_LONG;

						finalizeTransactionEntry(operation, rid, oldDataId, oldDataOffset);

						// CURRENT REQUESTER & TX: CLEAR THE ENTRY BY WRITING THE "FREE" STATUS
						file.writeByte(offset, STATUS_FREE);

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

				txId = file.readInt(offset);

				if (txId != iTxId)
					// NO MY TX, EXIT
					break;

				lastRecord = i;
				recordFreed--;
			}

			if (lastRecord > -1)
				file.shrink(lastRecord * RECORD_SIZE);
			
//			truncate();

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

	public void rollback(final OTransaction iTx) throws IOException {
		recoverTransaction(iTx.getId());
	}

	private void recoverTransactions() throws IOException {
		OLogManager.instance().debug(
				this,
				"Started the recovering of pending transactions after a hard shutdown. Found " + getTotalLogCount()
						+ " entry logs. Scanning...");

		int recoveredTxs = 0;
		int recoveredRecords = 0;
		int rec;

		final Set<Integer> txToRecover = scanForTransactionsToRecover();
		for (Integer txId : txToRecover) {
			rec = recoverTransaction(txId);

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

	private Set<Integer> scanForTransactionsToRecover() throws IOException {
		// SCAN ALL THE FILE SEARCHING FOR THE TRANSACTIONS TO RECOVER
		final Set<Integer> txToRecover = new HashSet<Integer>();
		final Set<Integer> txToNotRecover = new HashSet<Integer>();

		int size = file.getFilledUpTo() / RECORD_SIZE;
		for (int i = 0; i < size; ++i) {
			int offset = i * RECORD_SIZE;

			final byte status = file.readByte(offset);
			offset += OConstants.SIZE_BYTE;
			offset += OConstants.SIZE_BYTE;

			final int txId = file.readInt(offset);

			switch (status) {
			case STATUS_FREE:
				// NOT RECOVER IT SINCE IF FIND AT LEAST ONE "FREE" STATUS MEANS THAT ALL THE LOGS WAS COMMITTED BUT THE USER DIDN'T
				// RECEIVED THE ACK
				txToNotRecover.add(txId);
				break;

			case STATUS_COMMITTING:
				// TO RECOVER UNLESS THE REQ/TX IS IN THE MAP txToNotRecover
				txToRecover.add(txId);
				break;
			}
		}

		// FILTER THE TX MAP TO RECOVER BY REMOVING THE TX WITH AT LEAST ONE "FREE" STATUS
		txToRecover.removeAll(txToNotRecover);

		return txToRecover;
	}

	/**
	 * Scans the tx log file to find if any entry refers to the moved record. If yes move the reference too.
	 * 
	 * @param iDataSegmentId
	 *          Data segment id
	 * @param iSourcePosition
	 *          Moved record's source position
	 * @param iDestinationPosition
	 *          Moved record's destination position
	 * @throws IOException
	 */
	public void movedRecord(final int iDataSegmentId, final long iSourcePosition, final long iDestinationPosition) throws IOException {
		final int size = (file.getFilledUpTo() / RECORD_SIZE);
		for (int i = 0; i < size; ++i) {
			int offset = i * RECORD_SIZE;

			final byte status = file.readByte(offset);
			offset += OConstants.SIZE_BYTE;

			if (status != STATUS_FREE) {
				offset += OConstants.SIZE_BYTE;
				offset += OConstants.SIZE_INT;
				offset += OConstants.SIZE_SHORT;
				offset += OConstants.SIZE_LONG;

				final int oldDataId = file.readInt(offset);

				if (iDataSegmentId == oldDataId) {
					offset += OConstants.SIZE_INT;

					final long oldDataOffset = file.readLong(offset);

					if (oldDataOffset == iSourcePosition) {
						// UPDATE REFERENCE
						file.writeLong(offset, iDestinationPosition);
					}
				}
			}
		}
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
	private int recoverTransaction(final int iTxId) throws IOException {
		final OPhysicalPosition ppos = new OPhysicalPosition();

		int recordsRecovered = 0;
		final ORecordId rid = new ORecordId();

		final int size = (file.getFilledUpTo() / RECORD_SIZE);
		for (int i = 0; i < size; ++i) {
			int offset = i * RECORD_SIZE;

			final byte status = file.readByte(offset);
			offset += OConstants.SIZE_BYTE;

			if (status != STATUS_FREE) {
				// DIRTY TX LOG ENTRY
				final byte operation = file.readByte(offset);
				offset += OConstants.SIZE_BYTE;

				final int txId = file.readInt(offset);

				if (txId == iTxId) {
					// TX ID FOUND
					offset += OConstants.SIZE_INT;

					rid.clusterId = file.readShort(offset);
					offset += OConstants.SIZE_SHORT;

					rid.clusterPosition = file.readLong(offset);
					offset += OConstants.SIZE_LONG;

					final int oldDataId = file.readInt(offset);
					offset += OConstants.SIZE_INT;

					final long oldDataOffset = file.readLong(offset);
					offset += OConstants.SIZE_LONG;

					final int recordVersion = file.readInt(offset);

					recoverTransactionEntry(status, operation, txId, rid, oldDataId, oldDataOffset, recordVersion, ppos);
					recordsRecovered++;

					// CLEAR THE ENTRY BY WRITING '0'
					file.writeByte(i * RECORD_SIZE, STATUS_FREE);
				}
			}
		}
		return recordsRecovered;
	}

	public void finalizeTransactionEntry(final byte operation, final ORecordId iRid, final int oldDataId, final long oldDataOffset)
			throws IOException {
		final ODataLocal dataSegment = storage.getDataSegment(oldDataId);

		switch (operation) {
		case OPERATION_CREATE:
			break;

		case OPERATION_UPDATE:
			// CREATE A NEW HOLE FOR THE TEMPORARY OLD RECORD KEPT UNTIL COMMIT
			dataSegment.deleteRecord(oldDataOffset);
			break;

		case OPERATION_DELETE:
			// CREATE A NEW HOLE FOR THE TEMPORARY OLD RECORD KEPT UNTIL COMMIT
			dataSegment.deleteRecord(oldDataOffset);
			break;
		}
	}

	private void recoverTransactionEntry(final byte iStatus, final byte iOperation, final int iTxId, final ORecordId iRid,
			final int iOldDataId, final long iOldDataOffset, final int iRecordVersion, final OPhysicalPosition ppos) throws IOException {

		final OClusterLocal cluster = (OClusterLocal) storage.getClusterById(iRid.clusterId);

		if (!(cluster instanceof OClusterLocal))
			return;

		OLogManager.instance().debug(this, "Recovering tx <%d>. Operation <%d> was in status <%d> on record %s in data space %d...",
				iTxId, iOperation, iStatus, iRid, iOldDataOffset);

		switch (iOperation) {
		case OPERATION_CREATE:
			// JUST DELETE THE RECORD
			storage.deleteRecord(iRid, -1, null);
			break;

		case OPERATION_UPDATE:
			// RETRIEVE THE CURRENT PPOS
			cluster.getPhysicalPosition(iRid.clusterPosition, ppos);

			long oldPosition = ppos.dataPosition;

			// REPLACE THE POSITION AND SIZE OF THE OLD RECORD
			ppos.dataPosition = iOldDataOffset;
			ppos.recordSize = storage.getDataSegment(iOldDataId).getRecordSize(iOldDataOffset);

			// UPDATE THE PPOS WITH THE COORDS OF THE OLD RECORD
			cluster.setPhysicalPosition(iRid.clusterPosition, iOldDataId, iOldDataOffset, ppos.type, --ppos.version);
			storage.getDataSegment(iOldDataId).updateRid(iOldDataOffset, iRid);

			// CREATE A NEW HOLE ON THE NEW CREATED RECORD
			storage.getDataSegment(ppos.dataSegment).deleteRecord(oldPosition);
			break;

		case OPERATION_DELETE:
			// SAVE VERSION
			cluster.updateVersion(iRid.clusterPosition, iRecordVersion);
			cluster.removeHole(iRid.clusterPosition);
			break;
		}
	}

	private void synchRecord() throws IOException {
		if (((OStorageTxConfiguration) config).isSynchRecord())
			file.synch();
	}

	private void synchTx() throws IOException {
		if (((OStorageTxConfiguration) config).isSynchTx())
			file.synch();
	}
}
