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
package com.orientechnologies.orient.server.distributed.task;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedDatabaseChunk;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

/**
 * Ask for deployment of database from a remote node.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODeployDatabaseTask extends OAbstractReplicatedTask implements OCommandOutputListener {
  public final static int CHUNK_MAX_SIZE = 1048576; // 1MB

  public ODeployDatabaseTask() {
  }

  @Override
  public Object execute(final OServer iServer, ODistributedServerManager iManager, final ODatabaseDocumentTx database)
      throws Exception {

    if (!getNodeSource().equals(iManager.getLocalNodeName())) {

      final String databaseName = database.getName();

      final Lock lock = iManager.getLock(databaseName);
      if (lock.tryLock()) {
        try {
          // WAIT UNTIL ALL PENDING OPERATION ARE COMPLETED
          while (database.getStorage().getLastOperationId() >= iManager.getMessageService().getLastMessageId()) {
            ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
                "pausing deploy of database %s until all pending operations are completed...", databaseName);
            Thread.sleep(300);
          }

          iManager.setDatabaseStatus(databaseName, ODistributedServerManager.DB_STATUS.SYNCHRONIZING);

          ODistributedServerLog.warn(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT, "deploying database %s...",
              databaseName);

          final File f = new File(Orient.getTempPath() + "/backup_" + database.getName() + ".zip");
          if (f.exists())
            f.delete();
          else
            f.getParentFile().mkdirs();
          f.createNewFile();

          ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
              "creating backup of database '%s' in directory: %s...", databaseName, f.getAbsolutePath());

          final AtomicLong lastOperationId = new AtomicLong(-1);

          FileOutputStream fileOutputStream = new FileOutputStream(f);
          try {
            database.backup(fileOutputStream, null, new Callable<Object>() {
              @Override
              public Object call() throws Exception {
                lastOperationId.set(database.getStorage().getLastOperationId());
                return null;
              }
            }, this, OGlobalConfiguration.DISTRIBUTED_DEPLOYDB_TASK_COMPRESSION.getValueAsInteger(), CHUNK_MAX_SIZE);

            final long fileSize = f.length();

            ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
                "sending the compressed database '%s' over the NETWORK to node '%s', size=%s, lastOperationId=%d...", databaseName,
                getNodeSource(), OFileUtils.getSizeAsString(fileSize), lastOperationId.get());

            final ODistributedDatabaseChunk chunk = new ODistributedDatabaseChunk(lastOperationId.get(), f, 0, CHUNK_MAX_SIZE);

            ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), ODistributedServerLog.DIRECTION.OUT,
                "- transferring chunk #%d offset=%d size=%s...", 1, 0, OFileUtils.getSizeAsNumber(chunk.buffer.length));

            if (chunk.last)
              // NO MORE CHUNKS: SET THE NODE ONLINE (SYNCHRONIZING ENDED)
              iManager.setDatabaseStatus(databaseName, ODistributedServerManager.DB_STATUS.ONLINE);

            return chunk;

          } finally {
            fileOutputStream.close();
          }

        } finally {
          lock.unlock();

          ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), ODistributedServerLog.DIRECTION.OUT,
              "deploy database task completed");
        }

      } else
        ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.NONE,
            "skip deploying database %s because another node is doing it", databaseName);
    } else
      ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.NONE,
          "skip deploying database from the same node");

    return Boolean.FALSE;
  }

  @Override
  public RESULT_STRATEGY getResultStrategy() {
    return RESULT_STRATEGY.UNION;
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.NONE;
  }

  @Override
  public boolean isRequireNodeOnline() {
    return false;
  }

  @Override
  public long getTimeout() {
    return OGlobalConfiguration.DISTRIBUTED_DEPLOYDB_TASK_SYNCH_TIMEOUT.getValueAsLong();
  }

  @Override
  public String getPayload() {
    return null;
  }

  @Override
  public String getName() {
    return "deploy_db";
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
  }

  @Override
  public void onMessage(String iText) {
    if (iText.startsWith("\n"))
      iText = iText.substring(1);

    OLogManager.instance().info(this, iText);
  }

  @Override
  public boolean isRequiredOpenDatabase() {
    return false;
  }

}
