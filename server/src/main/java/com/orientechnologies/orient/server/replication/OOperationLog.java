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
package com.orientechnologies.orient.server.replication;

import java.io.IOException;
import java.util.logging.Level;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.storage.impl.local.OSingleFileSegment;
import com.orientechnologies.orient.server.clustering.OClusterLogger;
import com.orientechnologies.orient.server.clustering.OClusterLogger.DIRECTION;
import com.orientechnologies.orient.server.clustering.OClusterLogger.TYPE;

/**
 * Write all the operation during server cluster.<br/>
 * Uses the classic IO API and NOT the MMAP to avoid the buffer is not buffered by OS.<br/>
 * <br/>
 * Record structure:<br/>
 * <code>
 * +------------------------------------------------+<br/>
 * |........... FIXED SIZE AREA = 19 bytes .........|<br/>
 * +---------+--------+------------+----------------+<br/>
 * | SERIAL  | OPERAT | CLUSTER ID | CLUSTER OFFSET |<br/>
 * | 8 bytes | 1 byte | 2 bytes .. | 8 bytes ...... |<br/>
 * +---------+--------|------------+----------------+<br/>
 * = 19 bytes
 * </code><br/>
 */
public class OOperationLog extends OSingleFileSegment {
	public static final String		EXTENSION				= ".dol";
	private static final int			DEF_START_SIZE	= 262144;

	private static final int			OFFSET_SERIAL		= 0;
	private static final int			OFFSET_OPERAT		= OFFSET_SERIAL + OBinaryProtocol.SIZE_LONG;
	private static final int			OFFSET_RID			= OFFSET_OPERAT + OBinaryProtocol.SIZE_BYTE;
	private static final int			RECORD_SIZE			= OFFSET_RID + ORecordId.PERSISTENT_SIZE;

	private long									serial;
	private final String					nodeId;
	private final boolean					synchEnabled;
	private final OClusterLogger	logger					= new OClusterLogger();

	public OOperationLog(final String iNodeId, final String iDatabase, final boolean iReset) throws IOException {
		super(OReplicator.DIRECTORY_NAME + "/" + iDatabase + "/" + iNodeId.replace('.', '_').replace(':', '-') + EXTENSION,
				OGlobalConfiguration.DISTRIBUTED_LOG_TYPE.getValueAsString());
		nodeId = iNodeId;
		synchEnabled = OGlobalConfiguration.DISTRIBUTED_LOG_SYNCH.getValueAsBoolean();

		logger.setNode(nodeId);
		logger.setDatabase(iDatabase);

		file.setFailCheck(false);
		if (exists()) {
			if (iReset) {
				delete();
				create(DEF_START_SIZE);
			} else
				open();
		} else
			create(DEF_START_SIZE);

		serial = getLastOperationId() + 1;
	}

	/**
	 * Appends a log entry to the log file managed locally.
	 */
	public long appendLocalLog(final byte iOperation, final ORecordId iRID) throws IOException {

		acquireExclusiveLock();
		try {
			appendLog(serial, iOperation, iRID);
			return serial++;

		} finally {
			releaseExclusiveLock();
		}
	}

	/**
	 * Appends a log entry.
	 */
	public void appendLog(final long iSerial, final byte iOperation, final ORecordId iRID) throws IOException {

		acquireExclusiveLock();
		try {

			logger.log(this, Level.FINE, TYPE.REPLICATION, DIRECTION.NONE, "Journaled operation #%d as %s against record %s", iSerial,
					ORecordOperation.getName(iOperation), iRID);

			int offset = file.allocateSpace(RECORD_SIZE);

			file.writeLong(offset, iSerial);
			offset += OBinaryProtocol.SIZE_LONG;

			file.writeByte(offset, iOperation);
			offset += OBinaryProtocol.SIZE_BYTE;

			file.writeShort(offset, (short) iRID.clusterId);
			offset += OBinaryProtocol.SIZE_SHORT;

			file.writeLong(offset, iRID.clusterPosition);
			offset += OBinaryProtocol.SIZE_LONG;

			if (synchEnabled)
				file.synch();

		} finally {
			releaseExclusiveLock();
		}
	}

	public String getNodeId() {
		return nodeId;
	}

	public int findOperationId(final long iOperationId) throws IOException {
		if (iOperationId == -1)
			// SYNCH THE ENTIRE FILE
			return totalEntries() - 1;

		for (int i = totalEntries() - 1; i > -1; --i) {
			final long serial = file.readLong(i * RECORD_SIZE);
			if (serial == iOperationId)
				return i;
		}
		return -1;
	}

	public ORecordOperation getEntry(final int iPosition, final ORecordOperation iEntry) throws IOException {
		final int pos = iPosition * RECORD_SIZE;

		iEntry.serial = file.readLong(pos);
		iEntry.type = file.readByte(pos + OFFSET_OPERAT);
		iEntry.record = new ORecordId(file.readShort(pos + OFFSET_RID), file.readLong(pos + OFFSET_RID + OBinaryProtocol.SIZE_SHORT));
		return iEntry;
	}

	public long getFirstOperationId() throws IOException {
		if (isEmpty())
			return -1;

		return file.readLong(0);
	}

	public long getLastOperationId() throws IOException {
		if (isEmpty())
			return -1;

		return file.readLong(file.getFilledUpTo() - RECORD_SIZE);
	}

	public boolean isEmpty() {
		return file.getFilledUpTo() == 0;
	}

	public int totalEntries() {
		return file.getFilledUpTo() / RECORD_SIZE;
	}
}
