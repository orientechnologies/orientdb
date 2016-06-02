/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.server.distributed.impl.task;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.impl.ODistributedDatabaseChunk;
import com.orientechnologies.orient.server.distributed.impl.ODistributedStorage;
import com.orientechnologies.orient.server.distributed.task.OAbstractReplicatedTask;

import java.io.*;
import java.util.UUID;
import java.util.concurrent.locks.Lock;

/**
 * Ask for synchronization of database from a remote node.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OSyncDatabaseTask extends OAbstractReplicatedTask implements OCommandOutputListener {
  public final static int    CHUNK_MAX_SIZE = 8388608;    // 8MB
  public static final String DEPLOYDB       = "deploydb.";
  public static final int    FACTORYID      = 14;

  protected long             random;

  public OSyncDatabaseTask() {
    random = UUID.randomUUID().getLeastSignificantBits();
  }

  @Override
  public Object execute(final ODistributedRequestId requestId, final OServer iServer, final ODistributedServerManager iManager,
      final ODatabaseDocumentInternal database) throws Exception {

    if (!getNodeSource().equals(iManager.getLocalNodeName())) {
      if (database == null)
        throw new ODistributedException("Database instance is null");

      final String databaseName = database.getName();

      final Lock lock = iManager.getLock("sync." + databaseName);
      if (lock.tryLock()) {
        try {

          final Long lastDeployment = (Long) iManager.getConfigurationMap().get(DEPLOYDB + databaseName);
          if (lastDeployment != null && lastDeployment.longValue() == random) {
            // SKIP IT
            ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.NONE,
                "skip deploying database '%s' because already executed", databaseName);
            return Boolean.FALSE;
          }

          iManager.getConfigurationMap().put(DEPLOYDB + databaseName, random);

          iManager.setDatabaseStatus(getNodeSource(), databaseName, ODistributedServerManager.DB_STATUS.SYNCHRONIZING);
          iManager.setDatabaseStatus(iManager.getLocalNodeName(), databaseName, ODistributedServerManager.DB_STATUS.SYNCHRONIZING);

          ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT, "Deploying database %s...",
              databaseName);

          OLogSequenceNumber endLSN = null;

          File backupFile = ((ODistributedStorage) database.getStorage()).getLastValidBackup();

          if (backupFile == null || !backupFile.exists()) {
            // CREATE A BACKUP OF DATABASE FROM SCRATCH
            backupFile = new File(Orient.getTempPath() + "/backup_" + database.getName() + ".zip");

            final int compressionRate = OGlobalConfiguration.DISTRIBUTED_DEPLOYDB_TASK_COMPRESSION.getValueAsInteger();

            ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
                "Creating backup of database '%s' (compressionRate=%d) in directory: %s...", databaseName, compressionRate,
                backupFile.getAbsolutePath());

            if (backupFile.exists())
              backupFile.delete();
            else
              backupFile.getParentFile().mkdirs();
            backupFile.createNewFile();

            final FileOutputStream fileOutputStream = new FileOutputStream(backupFile);

            final File completedFile = new File(backupFile.getAbsolutePath() + ".completed");
            if (completedFile.exists())
              completedFile.delete();

            endLSN = ((OAbstractPaginatedStorage) database.getStorage().getUnderlying()).getLSN();

            new Thread(new Runnable() {
              @Override
              public void run() {
                Thread.currentThread().setName("OrientDB SyncDatabase node=" + iManager.getLocalNodeName() + " db=" + databaseName);

                try {
                  database.backup(fileOutputStream, null, null, new OCommandOutputListener() {
                    @Override
                    public void onMessage(String iText) {
                      if (iText.startsWith("\n"))
                        iText = iText.substring(1);

                      OLogManager.instance().info(this, iText);
                    }
                  }, OGlobalConfiguration.DISTRIBUTED_DEPLOYDB_TASK_COMPRESSION.getValueAsInteger(), CHUNK_MAX_SIZE);

                  ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
                      "Backup of database '%s' completed. lastOperationId=%s...", databaseName, requestId);

                } catch (IOException e) {
                  OLogManager.instance().error(this, "Cannot execute backup of database '%s' for deploy database", e, databaseName);
                } finally {
                  try {
                    fileOutputStream.close();
                  } catch (IOException e) {
                  }

                  try {
                    completedFile.createNewFile();
                  } catch (IOException e) {
                    OLogManager.instance().error(this, "Cannot create file of backup completed: %s", e, completedFile);
                  }
                }
              }
            }).start();

            // RECORD LAST BACKUP TO BE REUSED IN CASE ANOTHER NODE ASK FOR THE SAME IN SHORT TIME WHILE THE DB IS NOT UPDATED
            ((ODistributedStorage) database.getStorage()).setLastValidBackup(backupFile);

          } else {
            ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
                "Reusing last backup of database '%s' in directory: %s...", databaseName, backupFile.getAbsolutePath());
          }

          final ODistributedDatabaseChunk chunk = new ODistributedDatabaseChunk(backupFile, 0, CHUNK_MAX_SIZE, endLSN, false);

          ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), ODistributedServerLog.DIRECTION.OUT,
              "- transferring chunk #%d offset=%d size=%s...", 1, 0, OFileUtils.getSizeAsNumber(chunk.buffer.length));

          if (chunk.last)
            // NO MORE CHUNKS: SET THE NODE ONLINE (SYNCHRONIZING ENDED)
            iManager.setDatabaseStatus(iManager.getLocalNodeName(), databaseName, ODistributedServerManager.DB_STATUS.ONLINE);

          return chunk;

        } finally {
          lock.unlock();

          ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), ODistributedServerLog.DIRECTION.OUT,
              "Deploy database task completed");
        }

      } else
        ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.NONE,
            "Skip deploying database %s because another node is doing it", databaseName);
    } else
      ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.NONE,
          "Skip deploying database from the same node");

    return Boolean.FALSE;
  }

  @Override
  public RESULT_STRATEGY getResultStrategy() {
    return RESULT_STRATEGY.UNION;
  }

  @Override
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.NONE;
  }

  @Override
  public long getDistributedTimeout() {
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
    out.writeLong(random);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    random = in.readLong();
  }

  @Override
  public void onMessage(String iText) {
    if (iText.startsWith("\r\n"))
      iText = iText.substring(2);
    if (iText.startsWith("\n"))
      iText = iText.substring(1);

    OLogManager.instance().info(this, iText);
  }

  @Override
  public boolean isNodeOnlineRequired() {
    return false;
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }

}
