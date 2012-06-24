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
import java.util.LinkedList;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptiveExternal;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.fs.OFile;
import com.orientechnologies.orient.core.storage.fs.OFileFactory;
import com.orientechnologies.orient.server.task.OAbstractDistributedTask;
import com.orientechnologies.orient.server.task.OAbstractRecordDistributedTask;
import com.orientechnologies.orient.server.task.OCreateRecordDistributedTask;
import com.orientechnologies.orient.server.task.ODeleteRecordDistributedTask;
import com.orientechnologies.orient.server.task.OSQLCommandDistributedTask;
import com.orientechnologies.orient.server.task.OUpdateRecordDistributedTask;

/**
 * Writes all the non-idempotent operations against a database. Uses the classic IO API and NOT the MMAP to avoid the buffer is not
 * buffered by OS. The record is at variable size.<br/>
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
 *   <li> <b>STATUS</b> = [ 0 = doing, 1 = done ] </li>
 *   <li> <b>OPERAT</b> = [ 1 = update, 2 = delete, 3 = create, 4 = sql command ] </li>
 *   <li> <b>RUN ID</b> = is the running id. It's the timestamp the server is started, or inside a cluster is the timestamp when the cluster is started</li>
 *   <li> <b>OPERAT ID</b> = is the unique id of the operation. First operation is 0</li>
 * </ul>
 * </code><br/>
 */
public class ODatabaseJournal {
  public enum OPERATION_TYPES {
    RECORD_CREATE, RECORD_UPDATE, RECORD_DELETE, SQL_COMMAND
  }

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

  private OSharedResourceAdaptiveExternal lock                  = new OSharedResourceAdaptiveExternal(
                                                                    OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean(),
                                                                    0, true);
  private OStorage                        storage;
  private OFile                           file;
  private boolean                         synchEnabled          = false;

  public ODatabaseJournal(final OStorage iStorage, final String iStartingDirectory) throws IOException {
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

  /**
   * Returns the last operation id.
   */
  public long[] getLastOperationId() throws IOException {
    return getOperationId(file.getFilledUpTo());
  }

  /**
   * Returns the last operation id.
   */
  public long[] getOperationId(final long iOffset) throws IOException {
    final int filled = file.getFilledUpTo();
    if (filled == 0 || iOffset <= 0 || iOffset > filled)
      return new long[] { -1, -1 };

    lock.acquireExclusiveLock();
    try {

      final long[] ids = new long[2];
      ids[0] = file.readLong(iOffset - OFFSET_BACK_RUNID);
      ids[1] = file.readLong(iOffset - OFFSET_BACK_OPERATID);

      return ids;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  /**
   * Moves backward from the end of the file until the record id is major than the remote one collecting the positions
   * 
   * @param iRemoteLastOperationId
   * @return
   * @throws IOException
   */
  public Iterator<Long> browse(final long[] iRemoteLastOperationId) throws IOException {
    final LinkedList<Long> result = new LinkedList<Long>();

    long[] localOperationId = getLastOperationId();

    lock.acquireExclusiveLock();
    try {

      long fileOffset = file.getFilledUpTo();
      while ((localOperationId[0] > iRemoteLastOperationId[0])
          || (localOperationId[0] == iRemoteLastOperationId[0] && localOperationId[1] > iRemoteLastOperationId[1])) {

        // COLLECT CURRENT POSITION AS GOOD
        result.add(fileOffset);

        final long prevOffset = getPreviousOperation(fileOffset);
        localOperationId = getOperationId(prevOffset);
        fileOffset = prevOffset;
      }
      return result.descendingIterator();

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  /**
   * Change the status of an operation
   */
  public void changeOperationStatus(final long iOffsetEndOperation, final ORecordId iRid) throws IOException {
    lock.acquireExclusiveLock();
    try {
      final int varSize = file.readInt(iOffsetEndOperation - OFFSET_BACK_SIZE);
      final long offset = iOffsetEndOperation - OFFSET_BACK_SIZE - varSize - OFFSET_VARDATA;

      file.write(offset + OFFSET_STATUS, new byte[] { 1 });

      if (iRid != null)
        // UPDATE THE CLUSTER POSITION: THIS IS THE CASE OF CREATE RECORD
        file.writeLong(offset + OFFSET_VARDATA + OBinaryProtocol.SIZE_SHORT, iRid.clusterPosition);

      file.synch();

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  /**
   * Appends a log entry about a command with status = 0 (doing).
   * 
   * @return The end of the record stored for this operation.
   */
  public long journalOperation(final long iRunId, final long iOperationId, final OPERATION_TYPES iOperationType,
      final Object iVarData) throws IOException {

    lock.acquireExclusiveLock();
    try {

      long offset = 0;
      int varSize = 0;

      switch (iOperationType) {
      case RECORD_CREATE:
      case RECORD_UPDATE:
      case RECORD_DELETE: {
        final OAbstractRecordDistributedTask<?> task = (OAbstractRecordDistributedTask<?>) iVarData;
        varSize = ORecordId.PERSISTENT_SIZE;
        final ORecordId rid = task.getRid();

        if (OLogManager.instance().isDebugEnabled())
          OLogManager.instance().debug(this, "Journaled operation %s %s as #%d.%d", iOperationType.toString(), rid, iRunId,
              iOperationId);

        offset = writeOperationLogHeader(iOperationType, varSize);

        file.writeShort(offset + OFFSET_VARDATA, (short) rid.clusterId);
        file.writeLong(offset + OFFSET_VARDATA + OBinaryProtocol.SIZE_SHORT, rid.clusterPosition);
        break;
      }

      case SQL_COMMAND: {
        final OCommandSQL cmd = (OCommandSQL) iVarData;
        final String cmdText = cmd.getText();
        final byte[] cmdBinary = cmdText.getBytes();
        varSize = cmdBinary.length;

        if (OLogManager.instance().isDebugEnabled())
          OLogManager.instance().debug(this, "Journaled operation %s '%s' as #%d.%d", iOperationType.toString(), cmdText, iRunId,
              iOperationId);

        offset = writeOperationLogHeader(iOperationType, varSize);

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

  protected long writeOperationLogHeader(final OPERATION_TYPES iOperationType, final int varSize) throws IOException {
    final long offset = file.allocateSpace(FIXED_SIZE + varSize);
    file.writeByte(offset + OFFSET_STATUS, (byte) 0);
    file.writeByte(offset + OFFSET_OPERATION_TYPE, (byte) iOperationType.ordinal());
    file.writeInt(offset + OFFSET_VARDATA + varSize, varSize);
    return offset;
  }

  public OAbstractDistributedTask<?> getOperation(final long iOffsetEndOperation) throws IOException {
    OAbstractDistributedTask<?> task = null;

    lock.acquireExclusiveLock();
    try {

      final int varSize = file.readInt(iOffsetEndOperation - OFFSET_BACK_SIZE);
      final long offset = iOffsetEndOperation - OFFSET_BACK_SIZE - varSize - OFFSET_VARDATA;

      final OPERATION_TYPES operationType = OPERATION_TYPES.values()[file.readByte(offset + OFFSET_OPERATION_TYPE)];

      switch (operationType) {
      case RECORD_CREATE: {
        final ORecordId rid = new ORecordId(file.readShort(offset + OFFSET_VARDATA), file.readLong(offset + OFFSET_VARDATA
            + OBinaryProtocol.SIZE_SHORT));

        if (rid.isNew())
          // GET LAST RID
          rid.clusterPosition = storage.getClusterDataRange(rid.clusterId)[1];

        final ORawBuffer record = storage.readRecord(rid, null, false, null);
        if (record != null)
          task = new OCreateRecordDistributedTask(rid, record.buffer, record.version, record.recordType);
        break;
      }

      case RECORD_UPDATE: {
        final ORecordId rid = new ORecordId(file.readShort(offset + OFFSET_VARDATA), file.readLong(offset + OFFSET_VARDATA
            + OBinaryProtocol.SIZE_SHORT));

        final ORawBuffer record = storage.readRecord(rid, null, false, null);
        if (record != null)
          task = new OUpdateRecordDistributedTask(rid, record.buffer, record.version, record.recordType);
        break;
      }

      case RECORD_DELETE: {
        final ORecordId rid = new ORecordId(file.readShort(offset + OFFSET_VARDATA), file.readLong(offset + OFFSET_VARDATA
            + OBinaryProtocol.SIZE_SHORT));
        final ORawBuffer record = storage.readRecord(rid, null, false, null);
        if (record != null)
          task = new ODeleteRecordDistributedTask(rid, record.version);
        break;
      }

      case SQL_COMMAND: {
        final byte[] buffer = new byte[varSize];
        file.read(offset + OFFSET_VARDATA, buffer, buffer.length);
        task = new OSQLCommandDistributedTask(new String(buffer));
        break;
      }
      }

      task.setRedistribute(false);

    } finally {
      lock.releaseExclusiveLock();
    }
    return task;
  }

  public long getPreviousOperation(final long iPosition) throws IOException {
    lock.acquireExclusiveLock();
    try {

      final int size = file.readInt(iPosition - OFFSET_BACK_SIZE);
      return iPosition - OFFSET_BACK_SIZE - size - OFFSET_VARDATA;

    } finally {
      lock.releaseExclusiveLock();
    }
  }
}
