/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.server.distributed;

import java.io.File;
import java.io.IOException;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptiveExternal;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.storage.impl.local.OSingleFileSegment;

/**
 * Write all the operation during server cluster.<br/>
 * Uses the classic IO API and NOT the MMAP to avoid the buffer is not buffered by OS.<br/>
 * <br/>
 * Record structure:<br/>
 * <code>
 * +------------------------------------------------+---------+<br/>
 * |.............. FIXED SIZE AREA = 23 bytes ................|<br/>
 * +--------+------------+----------------+---------+---------+<br/>
 * | OPERAT | CLUSTER ID | CLUSTER OFFSET | VERSION | DATE .. |<br/>
 * | 1 byte | 2 bytes .. | 8 bytes ...... | 4 bytes | 8 bytes |<br/>
 * +--------|------------+----------------+---------+---------+<br/>
 * = 23 bytes
 * </code><br/>
 */
public class OSynchronizationLog extends OSingleFileSegment {
  /**
   * 
   */
  public static final String              SYNCHRONIZATION_DIRECTORY = "${" + Orient.ORIENTDB_HOME + "}/synchronization/";
  public static final String              EXTENSION                 = ".dol";
  private static final int                DEF_START_SIZE            = 262144;

  private static final int                OFFSET_OPERAT             = 0;
  private static final int                OFFSET_RID                = OFFSET_OPERAT + OBinaryProtocol.SIZE_BYTE;
  private static final int                OFFSET_VERSION            = OFFSET_RID + ORecordId.PERSISTENT_SIZE;
  private static final int                OFFSET_DATE               = OFFSET_VERSION + OBinaryProtocol.SIZE_INT;
  private static final int                RECORD_SIZE               = OFFSET_DATE + OBinaryProtocol.SIZE_LONG;

  private boolean                         synchEnabled;
  private OSharedResourceAdaptiveExternal lock                      = new OSharedResourceAdaptiveExternal(
                                                                        OGlobalConfiguration.ENVIRONMENT_CONCURRENT
                                                                            .getValueAsBoolean(),
                                                                        0, true);
  private final long                      limit;
  private long                            pendingLogs               = 0;
  private final ODistributedServerManager manager;
  private final String                    databaseName;
  private final String                    nodeId;

  public OSynchronizationLog(final ODistributedServerManager iManager, final String iNodeId, final String iDatabase,
      final long iLimit) throws IOException {
    super(SYNCHRONIZATION_DIRECTORY + iDatabase + "/" + iNodeId.replace('.', '_').replace(':', '-') + EXTENSION,
        OGlobalConfiguration.DISTRIBUTED_LOG_TYPE.getValueAsString());
    databaseName = iDatabase;
    nodeId = iNodeId;
    synchEnabled = OGlobalConfiguration.DISTRIBUTED_LOG_SYNCH.getValueAsBoolean();
    manager = iManager;

    file.setFailCheck(false);
    if (exists()) {
      open();
    } else
      create(DEF_START_SIZE);
    limit = iLimit;
  }

  public OSynchronizationLog(final ODistributedServerManager iManager, final File iFile, final String iDatabase, final long iLimit)
      throws IOException {
    super(iFile.getPath(), OGlobalConfiguration.DISTRIBUTED_LOG_TYPE.getValueAsString());
    databaseName = iDatabase;
    nodeId = iFile.getName().substring(0, iFile.getName().length() - EXTENSION.length()).replace('_', '.').replace('-', ':');
    synchEnabled = OGlobalConfiguration.DISTRIBUTED_LOG_SYNCH.getValueAsBoolean();
    manager = iManager;

    file.setFailCheck(false);
    if (exists()) {
      open();
    } else
      create(DEF_START_SIZE);
    limit = iLimit;
  }

  @Override
  public boolean open() throws IOException {
    final boolean result = super.open();

    final int tot = totalEntries();
    for (int i = 0; i < tot; ++i) {
      final byte operation = file.readByte(i * RECORD_SIZE);
      if (operation > -1)
        pendingLogs++;
    }

    if (pendingLogs > 0)
      OLogManager.instance().warn(this, "Synchronization log %s/%s found %d records to align once the node is online again",
          nodeId, databaseName, pendingLogs);

    return result;
  }

  @Override
  public void create(final int iStartSize) throws IOException {
    super.create(iStartSize);
    pendingLogs = 0;
  }

  /**
   * Reset the file
   */
  public void success() throws IOException {
    lock.acquireExclusiveLock();
    try {

      pendingLogs--;
      if (pendingLogs == 0 && file.getFreeSpace() < RECORD_SIZE)
        file.shrink(0);

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  /**
   * Resets the current entry to avoid re-sending if align interrupts for any reason
   * 
   * @param iPosition
   * @return
   * @throws IOException
   */
  public long alignedEntry(final int iPosition) throws IOException {
    lock.acquireExclusiveLock();
    try {

      final int pos = iPosition * RECORD_SIZE;
      file.writeByte(pos, (byte) -1);
      return --pendingLogs;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  /**
   * Appends a log entry until reach the configured limit if any (>-1)
   */
  public long appendLog(final byte iOperation, final ORecordId iRID, final int iVersion) throws IOException {

    lock.acquireExclusiveLock();
    try {
      final long serial = totalEntries();

      if (limit > -1 && serial > limit)
        return -1;

      pendingLogs++;

      if (pendingLogs > 1)
        OLogManager.instance().warn(this, "Journaled operation #%d as %s against %s/%s record %s. Remain to synchronize: %d",
            serial, ORecordOperation.getName(iOperation), nodeId, databaseName, iRID, pendingLogs);
      else
        OLogManager.instance().debug(this, "Journaled operation #%d as %s against %s/%s record %s", serial,
            ORecordOperation.getName(iOperation), nodeId, databaseName, iRID);

      int offset = file.allocateSpace(RECORD_SIZE);

      file.writeByte(offset, iOperation);
      offset += OBinaryProtocol.SIZE_BYTE;

      file.writeShort(offset, (short) iRID.clusterId);
      offset += OBinaryProtocol.SIZE_SHORT;

      file.writeLong(offset, iRID.clusterPosition);
      offset += OBinaryProtocol.SIZE_LONG;

      file.writeInt(offset, iVersion);
      offset += OBinaryProtocol.SIZE_INT;

      // WRITE THE TIME AS CLUSTER TIME (COMPUTE THE OFFSET)
      file.writeLong(offset, System.currentTimeMillis() + manager.getTimeOffset());

      if (synchEnabled)
        file.synch();

      return serial;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  /**
   * Updates last log entry. This makes sense only for new records because the RID is assigned only after the creation.
   */
  public void updateLog(final ORecordId iRID, final int iVersion) throws IOException {

    lock.acquireExclusiveLock();
    try {
      if (OLogManager.instance().isDebugEnabled())
        OLogManager.instance().debug(this, "Updated journaled operation #%d against %s/%s record %s version %d",
            totalEntries() - 1, nodeId, databaseName, iRID, iVersion);

      int offset = file.getFilledUpTo() - RECORD_SIZE;

      file.writeLong(offset + OFFSET_RID + OBinaryProtocol.SIZE_SHORT, iRID.clusterPosition);
      file.writeInt(offset + OFFSET_VERSION, iVersion);

      if (synchEnabled)
        file.synch();

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public ORecordOperation getEntry(final int iPosition, final ORecordOperation iEntry) throws IOException {
    lock.acquireExclusiveLock();
    try {

      final int pos = iPosition * RECORD_SIZE;

      iEntry.type = file.readByte(pos);
      iEntry.record = new ORecordId(file.readShort(pos + OFFSET_RID), file.readLong(pos + OFFSET_RID + OBinaryProtocol.SIZE_SHORT));
      iEntry.version = file.readInt(pos + OFFSET_VERSION);
      iEntry.date = file.readLong(pos + OFFSET_DATE);
      return iEntry;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public boolean needAlignment() {
    lock.acquireSharedLock();
    try {
      return pendingLogs > 0;
    } finally {
      lock.releaseSharedLock();
    }
  }

  public int totalEntries() {
    lock.acquireSharedLock();
    try {
      return file.getFilledUpTo() / RECORD_SIZE;
    } finally {
      lock.releaseSharedLock();
    }
  }

  public String getNodeId() {
    return nodeId;
  }
}
