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
import com.orientechnologies.orient.core.storage.fs.OFileFactory;
import com.orientechnologies.orient.core.tx.OTransaction;

/**
 * Handles the records that wait to be committed. This class is not synchronized because the caller is responsible of it.<br/>
 * Uses the classic IO API and NOT the MMAP to avoid the buffer is not buffered by OS.<br/>
 * <br/>
 * Record structure:<br/>
 * <code>
 * +-----------------------------------------------------------------------------------------+--------------------+<br/>
 * | .................... FIXED SIZE AREA = 24 bytes ....................................... | VARIABLE SIZE AREA |<br/>
 * +--------+--------+---------+------------+----------------+--------+--------+-------------+--------------------+<br/>
 * | STATUS | OPERAT | TX ID . | CLUSTER ID | CLUSTER OFFSET | TYPE . |VERSION | RECORD SIZE | RECORD CONTENT ... |<br/>
 * | 1 byte | 1 byte | 4 bytes | 2 bytes .. | 8 bytes ...... | 1 byte |4 bytes | 4 bytes ... | ? bytes .......... |<br/>
 * +--------+--------|---------+------------+----------------+--------+--------+-------------+--------------------+<br/>
 * > 25 bytes
 * </code><br/>
 * At commit time all the changes are written in the TX log file with status = STATUS_COMMITTING. Once all records have been
 * written, then the status of all the records is changed in STATUS_FREE. If a transactions has at least a STATUS_FREE means that
 * has been successfully committed. This is the reason why on startup all the pending transactions will be recovered, but those with
 * at least one record with status = STATUS_FREE.
 */
public class OTxSegment extends OSingleFileSegment {
	public static final byte	STATUS_FREE						= 0;
	public static final byte	STATUS_COMMITTING			= 1;

	public static final byte	OPERATION_CREATE			= 0;
	public static final byte	OPERATION_DELETE			= 1;
	public static final byte	OPERATION_UPDATE			= 2;

	private static final int	DEF_START_SIZE				= 262144;

	private static final int	OFFSET_TX_ID					= 2;
	private static final int	OFFSET_RECORD_SIZE		= 21;
	private static final int	OFFSET_RECORD_CONTENT	= 25;

	public OTxSegment(final OStorageLocal iStorage, final OStorageTxConfiguration iConfig) throws IOException {
		super(iStorage, iConfig, OFileFactory.CLASSIC);
	}

	/**
	 * Opens the file segment and recovers pending transactions if any
	 */
	@Override
	public boolean open() throws IOException {
		acquireExclusiveLock();

		try {
			// IGNORE IF IT'S SOFTLY CLOSED
			super.open();

			// CHECK FOR PENDING TRANSACTION ENTRIES TO RECOVER
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
	 * Appends a log entry
	 */
	public void addLog(final byte iOperation, final int iTxId, final int iClusterId, final long iClusterOffset,
			final byte iRecordType, final int iRecordVersion, final byte[] iRecordContent) throws IOException {

		final int contentSize = iRecordContent != null ? iRecordContent.length : 0;

		acquireExclusiveLock();
		try {
			final int size = OFFSET_RECORD_CONTENT + contentSize;

			int offset = file.allocateSpace(size);

			file.writeByte(offset, STATUS_COMMITTING);
			offset += OConstants.SIZE_BYTE;

			file.writeByte(offset, iOperation);
			offset += OConstants.SIZE_BYTE;

			file.writeInt(offset, iTxId);
			offset += OConstants.SIZE_INT;

			file.writeShort(offset, (short) iClusterId);
			offset += OConstants.SIZE_SHORT;

			file.writeLong(offset, iClusterOffset);
			offset += OConstants.SIZE_LONG;

			file.writeByte(offset, iRecordType);
			offset += OConstants.SIZE_BYTE;

			file.writeInt(offset, iRecordVersion);
			offset += OConstants.SIZE_INT;

			file.writeInt(offset, contentSize);
			offset += OConstants.SIZE_INT;

			file.write(offset, iRecordContent);
			offset += contentSize;

			file.synch();

		} finally {
			releaseExclusiveLock();
		}
	}

	/**
	 * Clears the entire file.
	 * 
	 * @param iReqId
	 *          The id of requester
	 * @param iTxId
	 *          The id of transaction
	 * 
	 * @throws IOException
	 */
	public void clearLogEntries(final int iTxId) throws IOException {
		truncate();
	}

	public void rollback(final OTransaction iTx) throws IOException {
		recoverTransaction(iTx.getId());
	}

	private void recoverTransactions() throws IOException {
		if (file.getFilledUpTo() == 0)
			return;

		OLogManager.instance().debug(this, "Started the recovering of pending transactions after a hard shutdown. Scanning...");

		int recoveredTxs = 0;
		int recoveredRecords = 0;
		int recs;

		final Set<Integer> txToRecover = scanForTransactionsToRecover();
		for (Integer txId : txToRecover) {
			recs = recoverTransaction(txId);

			if (recs > 0) {
				recoveredTxs++;
				recoveredRecords += recs;
			}
		}

		// EMPTY THE FILE
		file.shrink(0);

		if (recoveredRecords > 0) {
			OLogManager.instance().warn(this, "Recovering successfully completed:");
			OLogManager.instance().warn(this, "- Recovered Tx.....: " + recoveredTxs);
			OLogManager.instance().warn(this, "- Recovered Records: " + recoveredRecords);
		} else
			OLogManager.instance().debug(this, "Recovering successfully completed: no pending tx records found.");

	}

	/**
	 * Scans the segment and returns the set of transactions ids to recover.
	 */
	private Set<Integer> scanForTransactionsToRecover() throws IOException {
		// SCAN ALL THE FILE SEARCHING FOR THE TRANSACTIONS TO RECOVER
		final Set<Integer> txToRecover = new HashSet<Integer>();

		final Set<Integer> txToNotRecover = new HashSet<Integer>();

		// BROWSE ALL THE ENTRIES
		for (long offset = 0; eof(offset); offset = nextEntry(offset)) {
			// READ STATUS
			final byte status = file.readByte(offset);

			// READ TX-ID
			final int txId = file.readInt(offset + OFFSET_TX_ID);

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

		if (txToNotRecover.size() > 0)
			// FILTER THE TX MAP TO RECOVER BY REMOVING THE TX WITH AT LEAST ONE "FREE" STATUS
			txToRecover.removeAll(txToNotRecover);

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
	private int recoverTransaction(final int iTxId) throws IOException {
		final OPhysicalPosition ppos = new OPhysicalPosition();

		int recordsRecovered = 0;
		final ORecordId rid = new ORecordId();

		// BROWSE ALL THE ENTRIES
		for (long beginEntry = 0; eof(beginEntry); beginEntry = nextEntry(beginEntry)) {
			long offset = beginEntry;

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

					final byte recordType = file.readByte(offset);
					offset += OConstants.SIZE_BYTE;

					final int recordVersion = file.readInt(offset);
					offset += OConstants.SIZE_INT;

					final int recordSize = file.readInt(offset);
					offset += OConstants.SIZE_INT;

					final byte[] buffer;
					if (recordSize > 0) {
						buffer = new byte[recordSize];
						file.read(offset, buffer, recordSize);
						offset += recordSize;
					} else
						buffer = null;

					recoverTransactionEntry(status, operation, txId, rid, recordType, recordVersion, buffer, ppos);
					recordsRecovered++;

					// CLEAR THE ENTRY BY WRITING '0'
					file.writeByte(beginEntry, STATUS_FREE);
				}
			}
		}
		return recordsRecovered;
	}

	private void recoverTransactionEntry(final byte iStatus, final byte iOperation, final int iTxId, final ORecordId iRid,
			final byte iRecordType, final int iRecordVersion, final byte[] iRecordContent, final OPhysicalPosition ppos)
			throws IOException {

		final OClusterLocal cluster = (OClusterLocal) storage.getClusterById(iRid.clusterId);

		if (!(cluster instanceof OClusterLocal))
			return;

		OLogManager.instance().debug(this, "Recovering tx <%d>. Operation <%d> was in status <%d> on record %s size=%d...", iTxId,
				iOperation, iStatus, iRid, iRecordContent != null ? iRecordContent.length : 0);

		switch (iOperation) {
		case OPERATION_CREATE:
			// JUST DELETE THE RECORD
			storage.deleteRecord(iRid, -1, null);
			break;

		case OPERATION_UPDATE:
			// REPLACE WITH THE OLD ONE
			storage.updateRecord(cluster, iRid, iRecordContent, -2, iRecordType);
			break;

		case OPERATION_DELETE:
			// REMOVE THE HOLE
			cluster.removeHole(iRid.clusterPosition);
			cluster.updateBoundsAfterInsertion(iRid.clusterPosition);

			// RESTORE OLD CONTENT
			storage.updateRecord(cluster, iRid, iRecordContent, iRecordVersion, iRecordType);
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

	private boolean eof(final long iOffset) {
		return iOffset < file.getFilledUpTo();
	}

	private long nextEntry(final long iOffset) throws IOException {
		final int recordSize = file.readInt(iOffset + OFFSET_RECORD_SIZE);
		return iOffset + OFFSET_RECORD_CONTENT + recordSize;
	}
}
