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
package com.orientechnologies.orient.server.journal;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptiveExternal;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.fs.OFile;
import com.orientechnologies.orient.core.storage.fs.OFileFactory;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.task.OAbstractRecordReplicatedTask;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import com.orientechnologies.orient.server.distributed.task.OAbstractReplicatedTask;
import com.orientechnologies.orient.server.distributed.task.OCreateRecordTask;
import com.orientechnologies.orient.server.distributed.task.ODeleteRecordTask;
import com.orientechnologies.orient.server.distributed.task.OSQLCommandTask;
import com.orientechnologies.orient.server.distributed.task.OUpdateRecordTask;

/**
 * Writes all the non-idempotent operations against a database. Uses the classic IO API and NOT the MMAP to avoid the buffer is not
 * buffered by OS. The record is at variable size. To find the most recent the browsing begins from the end of file and go back. On
 * logging the status is always 0 (executing). Once the operation is executed the status is updated to 1 (executed).<br/>
 * <br/>
 * Record structure:<br/>
 * <code>
 * +--------+--------+---------------+---------+---------+-----------+<br/>
 * | STATUS | OPERAT | VARIABLE DATA | SIZE .. | RUN ID .| OPERAT ID |<br/>
 * | 1 byte | 1 byte | &lt;SIZE&gt; bytes.  | 4 bytes | 8 bytes | 8 bytes . |<br/>
 * +--------+--------+---------------+---------+---------+-----------+<br/>
 * FIXED SIZE = 22 <br/>
 * <br/>
 * Where:
 * <ul>
 * <li> <b>STATUS</b> = [ 0 = uncommitted, 1 = committed ] </li>
 * <li> <b>OPERAT</b> = [ 1 = update, 2 = delete, 3 = create, 4 = sql command ] </li>
 * <li> <b>RUN ID</b> = is the running id. It's the timestamp the server is started, or inside a cluster is the timestamp when the cluster is started</li>
 * <li> <b>OPERAT ID</b> = is the unique id of the operation. First operation is 0</li>
 * </ul>
 * </code><br/>
 */
public class ODatabaseJournal {
  public enum OPERATION_STATUS {
    UNCOMMITTED, COMMITTED, CANCELED
  }

  public enum OPERATION_TYPES {
    RECORD_CREATE, RECORD_UPDATE, RECORD_DELETE, SQL_COMMAND
  }

  private static final long[]             BEGIN_POSITION        = new long[] { -1, -1 };

  public static final String              DIRECTORY             = "log";
  public static final String              FILENAME              = "journal.olj";
  private static final int                DEF_START_SIZE        = 262144;

  private static final int                OFFSET_STATUS         = 0;
  private static final int                OFFSET_OPERATION_TYPE = OFFSET_STATUS + OBinaryProtocol.SIZE_BYTE;
  private static final int                OFFSET_VARDATA        = OFFSET_OPERATION_TYPE + OBinaryProtocol.SIZE_BYTE;

  private static final int                OFFSET_BACK_OPERATID  = OBinaryProtocol.SIZE_LONG;
  private static final int                OFFSET_BACK_RUNID     = OFFSET_BACK_OPERATID + OBinaryProtocol.SIZE_LONG;
  private static final int                OFFSET_BACK_SIZE      = OFFSET_BACK_RUNID + OBinaryProtocol.SIZE_INT;

  private static final int                FIXED_SIZE            = 22;

  private OServer                         server;
  private ODistributedServerManager       cluster;
  private OSharedResourceAdaptiveExternal lock                  = new OSharedResourceAdaptiveExternal(
                                                                    OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean(),
                                                                    0, true);
  private OStorage                        storage;
  private OFile                           file;
  private boolean                         synchEnabled          = false;

  public ODatabaseJournal(final OServer iServer, final ODistributedServerManager iCluster, final OStorage iStorage,
      final String iStartingDirectory) throws IOException {
    server = iServer;
    cluster = iCluster;
    storage = iStorage;

    File osFile = new File(iStartingDirectory + "/" + DIRECTORY);
    if (!osFile.exists())
      osFile.mkdirs();

    osFile = new File(iStartingDirectory + "/" + DIRECTORY + "/" + FILENAME);

    file = OFileFactory.instance().create("classic", osFile.getAbsolutePath(), "rw");
    if (file.exists())
      file.open();
    else
      file.create(DEF_START_SIZE);
  }

  public OStorage getStorage() {
    return storage;
  }

  public OAbstractReplicatedTask<?> getOperation(final long iOffsetEndOperation) throws IOException {
    OAbstractReplicatedTask<?> task = null;

    lock.acquireExclusiveLock();
    try {

      final long runId = file.readLong(iOffsetEndOperation - OFFSET_BACK_RUNID);
      final long operationId = file.readLong(iOffsetEndOperation - OFFSET_BACK_OPERATID);
      final int varSize = file.readInt(iOffsetEndOperation - OFFSET_BACK_SIZE);
      final long offset = iOffsetEndOperation - OFFSET_BACK_SIZE - varSize - OFFSET_VARDATA;

      final OPERATION_TYPES operationType = OPERATION_TYPES.values()[file.readByte(offset + OFFSET_OPERATION_TYPE)];

      switch (operationType) {
      case RECORD_CREATE: {
        final ORecordId rid = new ORecordId(file.readShort(offset + OFFSET_VARDATA), OClusterPositionFactory.INSTANCE.valueOf(file
            .readLong(offset + OFFSET_VARDATA + OBinaryProtocol.SIZE_SHORT)));

        if (rid.isNew())
          // GET LAST RID
          rid.clusterPosition = storage.getClusterDataRange(rid.clusterId)[1];

        final ORawBuffer record = storage.readRecord(rid, null, false, null, false).getResult();
        if (record != null)
          task = new OCreateRecordTask(runId, operationId, rid, record.buffer, record.version, record.recordType);
        break;
      }

      case RECORD_UPDATE: {
        final ORecordId rid = new ORecordId(file.readShort(offset + OFFSET_VARDATA), OClusterPositionFactory.INSTANCE.valueOf(file
            .readLong(offset + OFFSET_VARDATA + OBinaryProtocol.SIZE_SHORT)));

        final ORawBuffer record = storage.readRecord(rid, null, false, null, false).getResult();
        if (record != null) {
          final ORecordVersion version = record.version.copy();
          version.decrement();
          task = new OUpdateRecordTask(runId, operationId, rid, record.buffer, version, record.recordType);
        }
        break;
      }

      case RECORD_DELETE: {
        final ORecordId rid = new ORecordId(file.readShort(offset + OFFSET_VARDATA), OClusterPositionFactory.INSTANCE.valueOf(file
            .readLong(offset + OFFSET_VARDATA + OBinaryProtocol.SIZE_SHORT)));
        final ORawBuffer record = storage.readRecord(rid, null, false, null, false).getResult();
        task = new ODeleteRecordTask(runId, operationId, rid, record != null ? record.version : OVersionFactory.instance()
            .createUntrackedVersion());
        break;
      }

      case SQL_COMMAND: {
        final byte[] buffer = new byte[varSize];
        file.read(offset + OFFSET_VARDATA, buffer, buffer.length);
        task = new OSQLCommandTask(runId, operationId, new String(buffer));
        break;
      }
      }

      if (task != null)
        task.setServerInstance(server);

    } finally {
      lock.releaseExclusiveLock();
    }
    return task;
  }

  /**
   * Returns the last operation id with passed status
   */
  public long[] getLastOperationId(final OPERATION_STATUS iStatus) throws IOException {
    lock.acquireExclusiveLock();
    try {
      final int filled = file.getFilledUpTo();
      if (filled == 0)
        // RETURN THE BEGIN
        return BEGIN_POSITION;

      final Iterator<Long> iter = browseLastOperations(BEGIN_POSITION, iStatus, 1);

      if (iter == null || !iter.hasNext())
        // RETURN THE BEGIN
        return BEGIN_POSITION;

      final long[] ids = new long[2];
      while (iter.hasNext()) {
        long offset = iter.next();
        ids[0] = file.readLong(offset - OFFSET_BACK_RUNID);
        ids[1] = file.readLong(offset - OFFSET_BACK_OPERATID);
      }
      return ids;
    } finally {
      lock.releaseExclusiveLock();
    }
  }

  /**
   * Returns the last operation id.
   */
  public long[] getOperationId(final long iOffset) throws IOException {
    lock.acquireExclusiveLock();
    try {
      final int filled = file.getFilledUpTo();
      if (filled == 0 || iOffset <= 0 || iOffset > filled)
        return BEGIN_POSITION;

      final long[] ids = new long[2];
      ids[0] = file.readLong(iOffset - OFFSET_BACK_RUNID);
      ids[1] = file.readLong(iOffset - OFFSET_BACK_OPERATID);

      return ids;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  /**
   * Moves backward from the end of the file until the record id is major than the remote one collecting the positions with the same
   * status as passed
   * 
   * @param iRemoteLastOperationId
   *          Last remote operation. It works like a maximum point to not overcome.
   * @param iStatus
   *          Status to collect
   * @param iMax
   *          Maximum items to collect
   * @return
   * @throws IOException
   */
  public Iterator<Long> browseLastOperations(final long[] iRemoteLastOperationId, final OPERATION_STATUS iStatus, final int iMax)
      throws IOException {
    final LinkedList<Long> result = new LinkedList<Long>();

    lock.acquireExclusiveLock();
    try {
      long fileOffset = file.getFilledUpTo();

      long[] localOperationId = getOperationId(fileOffset);

      while ((localOperationId[0] > iRemoteLastOperationId[0])
          || (localOperationId[0] == iRemoteLastOperationId[0] && localOperationId[1] > iRemoteLastOperationId[1])) {

        if (getOperationStatus(fileOffset) == iStatus) {
          // COLLECT CURRENT POSITION AS GOOD
          result.add(fileOffset);

          if (iMax > -1 && result.size() >= iMax)
            // MAX LIMIT REACHED
            break;
        }

        final long prevOffset = getPreviousOperation(fileOffset);
        localOperationId = getOperationId(prevOffset);
        fileOffset = prevOffset;
      }
      return result.descendingIterator();

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public Map<ORecordId, Long> getUncommittedOperations() throws IOException {
    final Map<ORecordId, Long> uncommittedRecords = new LinkedHashMap<ORecordId, Long>();

    // FIND LAST COMMITTED OPERATION
    long fileOffset = file.getFilledUpTo();
    while (fileOffset > 0) {
      final OPERATION_STATUS status = getOperationStatus(fileOffset);
      if (status == OPERATION_STATUS.COMMITTED)
        // STOP
        break;

      if (status != OPERATION_STATUS.CANCELED) {
        final OAbstractRemoteTask<?> op = getOperation(fileOffset);

        ODistributedServerLog.info(this, cluster.getLocalNodeId(), null, DIRECTION.NONE, "db '%s' found uncommitted operation %s",
            storage.getName(), op);

        if (op instanceof OAbstractRecordReplicatedTask<?>)
          // COLLECT THE RECORD TO BE RETRIEVED FROM OTHER SERVERS
          uncommittedRecords.put(((OAbstractRecordReplicatedTask<?>) op).getRid(), fileOffset);
      }

      fileOffset = getPreviousOperation(fileOffset);
    }
    return uncommittedRecords;
  }

  /**
   * Changes the status of an operation. If RID is not null it's updated too (in case of record create when the RID changes).
   */
  public void setOperationStatus(final long iOffsetEndOperation, final ORecordId iRid, final OPERATION_STATUS iStatus)
      throws IOException {
    lock.acquireExclusiveLock();
    try {
      final int varSize = file.readInt(iOffsetEndOperation - OFFSET_BACK_SIZE);
      final long offset = iOffsetEndOperation - OFFSET_BACK_SIZE - varSize - OFFSET_VARDATA;

      ODistributedServerLog.debug(this, cluster.getLocalNodeId(), null, DIRECTION.NONE,
          "update journal db '%s' on operation #%d.%d rid %s", storage.getName(),
          file.readLong(iOffsetEndOperation - OFFSET_BACK_RUNID), file.readLong(iOffsetEndOperation - OFFSET_BACK_OPERATID), iRid);

      file.write(offset + OFFSET_STATUS, new byte[] { (byte) iStatus.ordinal() });

      if (iRid != null)
        // UPDATE THE CLUSTER POSITION: THIS IS THE CASE OF CREATE RECORD
        file.writeLong(offset + OFFSET_VARDATA + OBinaryProtocol.SIZE_SHORT, iRid.clusterPosition.longValue());

      file.synch();

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  /**
   * Returns the operation status as byte: 0 = EXECUTING, 1 = EXECUTED
   */
  private OPERATION_STATUS getOperationStatus(final long iOffsetEndOperation) throws IOException {
    final int varSize = file.readInt(iOffsetEndOperation - OFFSET_BACK_SIZE);
    final long offset = iOffsetEndOperation - OFFSET_BACK_SIZE - varSize - OFFSET_VARDATA;

    return OPERATION_STATUS.values()[file.readByte(offset + OFFSET_STATUS)];
  }

  /**
   * Appends a log entry about a command with status = 0 (doing).
   * 
   * @return The end of the record stored for this operation.
   */
  public long append(final OAbstractReplicatedTask<?> task) throws IOException {

    final OPERATION_TYPES iOperationType = task.getOperationType();
    final long iRunId = task.getRunId();
    final long iOperationId = task.getOperationSerial();

    lock.acquireExclusiveLock();
    try {

      long offset = 0;
      int varSize = 0;

      switch (iOperationType) {
      case RECORD_CREATE:
      case RECORD_UPDATE:
      case RECORD_DELETE: {
        varSize = ORecordId.PERSISTENT_SIZE;
        final ORecordId rid = ((OAbstractRecordReplicatedTask<?>) task).getRid();

        ODistributedServerLog.debug(this, cluster.getLocalNodeId(), null, DIRECTION.NONE,
            "journaled operation %s against db '%s' rid %s as #%d.%d", iOperationType.toString(), storage.getName(), rid, iRunId,
            iOperationId);

        if (isUpdatingLast(iRunId, iOperationId))
          offset = getOffset2Update(iRunId, iOperationId, iOperationType, varSize);
        else
          offset = appendOperationLogHeader(iOperationType, varSize);

        file.writeShort(offset + OFFSET_VARDATA, (short) rid.clusterId);
        file.writeLong(offset + OFFSET_VARDATA + OBinaryProtocol.SIZE_SHORT, rid.clusterPosition.longValue());
        break;
      }

      case SQL_COMMAND: {
        final OCommandSQL cmd = new OCommandSQL(task.getPayload());
        final String cmdText = cmd.getText();
        final byte[] cmdBinary = cmdText.getBytes();
        varSize = cmdBinary.length;

        ODistributedServerLog.debug(this, cluster.getLocalNodeId(), null, DIRECTION.NONE,
            "journaled operation %s against db '%s' cmd '%s' as #%d.%d", iOperationType.toString(), storage.getName(), cmdText,
            iRunId, iOperationId);

        offset = appendOperationLogHeader(iOperationType, varSize);

        file.write(offset + OFFSET_VARDATA, cmdText.getBytes());
        break;
      }
      }

      file.writeLong(offset + OFFSET_VARDATA + varSize + OBinaryProtocol.SIZE_INT, iRunId);
      file.writeLong(offset + OFFSET_VARDATA + varSize + OBinaryProtocol.SIZE_INT + OBinaryProtocol.SIZE_LONG, iOperationId);

      if (synchEnabled)
        file.synch();

      return offset + OFFSET_VARDATA + varSize + OBinaryProtocol.SIZE_INT + OBinaryProtocol.SIZE_LONG + OBinaryProtocol.SIZE_LONG;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  /**
   * Appends a new log by writing the header without the payload (variable data)
   * 
   * @throws IOException
   */
  protected long appendOperationLogHeader(final OPERATION_TYPES iOperationType, final int varSize) throws IOException {
    final long offset = file.allocateSpace(FIXED_SIZE + varSize);
    // UNCOMMITTED, WILL BE SET TO 1 (COMMITTED) ONCE LOCAL OPERATION SUCCEED
    file.writeByte(offset + OFFSET_STATUS, (byte) OPERATION_STATUS.UNCOMMITTED.ordinal());
    file.writeByte(offset + OFFSET_OPERATION_TYPE, (byte) iOperationType.ordinal());
    file.writeInt(offset + OFFSET_VARDATA + varSize, varSize);
    return offset;
  }

  /**
   * Check operationId overlapped, if so, then need overwrite
   * 
   * @param runId
   * @param operationId
   * @return
   * @throws IOException
   */
  protected boolean isUpdatingLast(final long runId, final long operationId) throws IOException {
    long offset = file.getFilledUpTo();
    if (offset > 0) {
      long localRunId = file.readLong(offset - OFFSET_BACK_RUNID);
      long localOperationId = file.readLong(offset - OFFSET_BACK_OPERATID);
      if (localRunId == runId && localOperationId >= operationId) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks all the journal entries to find the last record point.
   * 
   * @param runId
   * @param operationId
   * @param iOperationType
   * @param varSize
   * @return
   * @throws IOException
   */
  protected long getOffset2Update(final long runId, final long operationId, final OPERATION_TYPES iOperationType, final int varSize)
      throws IOException {
    long currentOffset = file.getFilledUpTo();
    boolean foundMatch = false;
    while (currentOffset > 0) {
      long localRunId = file.readLong(currentOffset - OFFSET_BACK_RUNID);
      long localOperationId = file.readLong(currentOffset - OFFSET_BACK_OPERATID);

      if (localRunId == runId && localOperationId == operationId /* && !this.getOperationStatus(currentOffset) */) {
        foundMatch = true;
        break;
      } else if (localOperationId < operationId || localRunId != runId)
        break;

      currentOffset = getPreviousOperation(currentOffset);
    }

    if (foundMatch) {
      int var = file.readInt(currentOffset - OFFSET_BACK_SIZE);
      return (currentOffset - OFFSET_BACK_SIZE - var - OFFSET_VARDATA);
    }

    return appendOperationLogHeader(iOperationType, varSize);
  }

  protected long getPreviousOperation(final long iPosition) throws IOException {
    final int size = file.readInt(iPosition - OFFSET_BACK_SIZE);
    return iPosition - OFFSET_BACK_SIZE - size - OFFSET_VARDATA;
  }
}
