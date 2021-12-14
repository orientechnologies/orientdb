/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.storage.impl.local.OSyncSource;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ORemoteTaskFactory;
import com.orientechnologies.orient.server.distributed.impl.ODistributedDatabaseChunk;
import com.orientechnologies.orient.server.distributed.impl.ODistributedDatabaseImpl;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Ask for a database chunk.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class OCopyDatabaseChunkTask extends OAbstractRemoteTask {
  private static final long serialVersionUID = 1L;
  public static final int FACTORYID = 15;

  private String fileName;
  private int chunkNum;
  private long offset;
  private boolean compressed;

  public OCopyDatabaseChunkTask() {}

  public OCopyDatabaseChunkTask(
      final String iFileName, final int iChunkNum, final long iOffset, final boolean iCompressed) {
    fileName = iFileName;
    chunkNum = iChunkNum;
    offset = iOffset;
    compressed = iCompressed;
  }

  @Override
  public Object execute(
      ODistributedRequestId requestId,
      final OServer iServer,
      ODistributedServerManager iManager,
      final ODatabaseDocumentInternal database)
      throws Exception {

    if (database == null) {
      throw new ODistributedException("database not available anymore during sync");
    }

    ODistributedDatabaseImpl local =
        (ODistributedDatabaseImpl) iManager.getMessageService().getDatabase(database.getName());

    OSyncSource b = local.getLastValidBackup();

    final ODistributedDatabaseChunk result =
        new ODistributedDatabaseChunk(b, OSyncDatabaseTask.CHUNK_MAX_SIZE);

    ODistributedServerLog.info(
        this,
        iManager.getLocalNodeName(),
        getNodeSource(),
        ODistributedServerLog.DIRECTION.OUT,
        "- transferring chunk #%d offset=%d size=%s...",
        chunkNum,
        result.offset,
        OFileUtils.getSizeAsNumber(result.buffer.length));

    return result;
  }

  @Override
  public RESULT_STRATEGY getResultStrategy() {
    return RESULT_STRATEGY.ANY;
  }

  @Override
  public long getDistributedTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_DEPLOYCHUNK_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.NONE;
  }

  @Override
  public String getName() {
    return "copy_db_chunk";
  }

  @Override
  public void toStream(final DataOutput out) throws IOException {
    out.writeUTF(fileName);
    out.writeInt(chunkNum);
    out.writeLong(offset);
  }

  @Override
  public void fromStream(final DataInput in, final ORemoteTaskFactory factory) throws IOException {
    fileName = in.readUTF();
    chunkNum = in.readInt();
    offset = in.readLong();
  }

  @Override
  public boolean isNodeOnlineRequired() {
    return false;
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }

  public String getFileName() {
    return fileName;
  }

  public int getChunkNum() {
    return chunkNum;
  }

  public long getOffset() {
    return offset;
  }

  public boolean isCompressed() {
    return compressed;
  }
}
