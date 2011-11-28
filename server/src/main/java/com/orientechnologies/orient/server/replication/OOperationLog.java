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

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.storage.impl.local.OSingleFileSegment;

/**
 * Write all the operation in cluster.<br/>
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
	public static final String	EXTENSION				= ".dol";
	private static final int		DEF_START_SIZE	= 262144;

	private static final int		OFFSET_SERIAL		= 0;
	private static final int		OFFSET_OPERAT		= 8;
	private static final int		OFFSET_RID			= 9;
	private static final int		RECORD_SIZE			= 19;

	private long								serial					= 0;
	private final String				nodeId;
	private final boolean				synchEnabled;

	public OOperationLog(final String iNodeId, final String iDatabase) throws IOException {
		super(OReplicator.DIRECTORY_NAME + "/" + iDatabase + "/" + iNodeId.replace('.', '_').replace(':', '-') + EXTENSION,
				OGlobalConfiguration.DISTRIBUTED_LOG_TYPE.getValueAsString());
		nodeId = iNodeId;
		synchEnabled = OGlobalConfiguration.DISTRIBUTED_LOG_SYNCH.getValueAsBoolean();

		file.setFailCheck(false);
		if (exists())
			open();
		else
			create(DEF_START_SIZE);
	}

	/**
	 * Appends a log entry
	 */
	public long addLog(final byte iOperation, final ORecordId iRID) throws IOException {

		acquireExclusiveLock();
		try {
			int offset = file.allocateSpace(file.getFilledUpTo() + RECORD_SIZE);

			file.writeLong(offset, serial);
			offset += OBinaryProtocol.SIZE_LONG;

			file.writeByte(offset, iOperation);
			offset += OBinaryProtocol.SIZE_BYTE;

			file.writeShort(offset, (short) iRID.clusterId);
			offset += OBinaryProtocol.SIZE_SHORT;

			file.writeLong(offset, iRID.clusterPosition);
			offset += OBinaryProtocol.SIZE_LONG;

			if (synchEnabled)
				file.synch();

			return serial++;

		} finally {
			releaseExclusiveLock();
		}
	}

	private boolean eof(final long iOffset) {
		return iOffset < file.getFilledUpTo();
	}

	public String getNodeId() {
		return nodeId;
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
