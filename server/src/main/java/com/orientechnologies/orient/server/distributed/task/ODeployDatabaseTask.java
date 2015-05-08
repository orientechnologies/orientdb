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
package com.orientechnologies.orient.server.distributed.task;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OStorageAbstract;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedDatabaseChunk;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ODistributedStorage;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
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
  public final static int    CHUNK_MAX_SIZE        = 1048576;        // 1MB
  public static final String DEPLOYDB              = "deploydb.";
  protected final ODocument  databaseConfiguration = new ODocument();
  protected long             random;

  public ODeployDatabaseTask() {
  }

  public ODeployDatabaseTask(final ODatabaseDocumentTx db) {
    random = UUID.randomUUID().getLeastSignificantBits();

    if (db != null) {
      // TO USE IN CASE OF CHECK CLUSTERS (SPLIT-NETWORK)
      final OStorageAbstract stg = (OStorageAbstract) db.getStorage().getUnderlying();

      final ODocument clusters = new ODocument();
      databaseConfiguration.field("clusters", clusters);

      for (String clName : stg.getClusterNames()) {
        final OCluster c = stg.getClusterByName(clName);

        final ODocument cluster = new ODocument();
        clusters.field(clName, cluster);

        try {
          cluster.field("records", c.getEntries());
          cluster.field("first", c.getFirstPosition());
          cluster.field("last", c.getFirstPosition());
        } catch (IOException e) {
          OLogManager.instance().error(this, "Error on deploying cluster " + clName, e);
        }
      }
    }
  }

  @Override
  public Object execute(final OServer iServer, ODistributedServerManager iManager, final ODatabaseDocumentTx database)
      throws Exception {

    if (!getNodeSource().equals(iManager.getLocalNodeName())) {
      if (database == null)
        throw new ODistributedException("Database instance is null");

      final String databaseName = database.getName();

      final Lock lock = iManager.getLock(databaseName);
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

          ODistributedServerLog.warn(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT, "deploying database %s...",
              databaseName);

          final AtomicLong lastOperationId = new AtomicLong(-1);

          File backupFile = ((ODistributedStorage) database.getStorage()).getLastValidBackup();

          if (backupFile == null || !backupFile.exists()) {
            // CREATE A BACKUP OF DATABASE FROM SCRATCH
            backupFile = new File(Orient.getTempPath() + "/backup_" + database.getName() + ".zip");

            ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
                "creating backup of database '%s' in directory: %s...", databaseName, backupFile.getAbsolutePath());

            if (backupFile.exists())
              backupFile.delete();
            else
              backupFile.getParentFile().mkdirs();
            backupFile.createNewFile();

            final FileOutputStream fileOutputStream = new FileOutputStream(backupFile);
            try {
              database.backup(fileOutputStream, null, new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                  lastOperationId.set(database.getStorage().getLastOperationId());
                  return null;
                }
              }, this, OGlobalConfiguration.DISTRIBUTED_DEPLOYDB_TASK_COMPRESSION.getValueAsInteger(), CHUNK_MAX_SIZE);
            } finally {
              fileOutputStream.close();
            }

            // RECORD LAST BACKUP TO BE REUSED IN CASE ANOTHER NODE ASK FOR THE SAME IN SHORT TIME WHILE THE DB IS NOT UPDATED
            ((ODistributedStorage) database.getStorage()).setLastValidBackup(backupFile);
          } else
            ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
                "reusing last backup of database '%s' in directory: %s...", databaseName, backupFile.getAbsolutePath());

          final long fileSize = backupFile.length();

          ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
              "sending the compressed database '%s' over the NETWORK to node '%s', size=%s, lastOperationId=%d...", databaseName,
              getNodeSource(), OFileUtils.getSizeAsString(fileSize), lastOperationId.get());

          final ODistributedDatabaseChunk chunk = new ODistributedDatabaseChunk(lastOperationId.get(), backupFile, 0,
              CHUNK_MAX_SIZE);

          ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), ODistributedServerLog.DIRECTION.OUT,
              "- transferring chunk #%d offset=%d size=%s...", 1, 0, OFileUtils.getSizeAsNumber(chunk.buffer.length));

          if (chunk.last)
            // NO MORE CHUNKS: SET THE NODE ONLINE (SYNCHRONIZING ENDED)
            iManager.setDatabaseStatus(iManager.getLocalNodeName(), databaseName, ODistributedServerManager.DB_STATUS.ONLINE);

          return chunk;

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
  public OCommandDistributedReplicateRequest.QUORUM_TYPE getQuorumType() {
    return OCommandDistributedReplicateRequest.QUORUM_TYPE.NONE;
  }

  @Override
  public boolean isRequireNodeOnline() {
    return true;
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
    out.writeLong(random);
    final byte[] buffer = databaseConfiguration.toStream();
    out.writeInt(buffer.length);
    out.write(buffer);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    random = in.readLong();
    final int bufferLength = in.readInt();
    final byte[] buffer = new byte[bufferLength];
    in.read(buffer);
    databaseConfiguration.fromStream(buffer);
  }

  @Override
  public void onMessage(String iText) {
    if (iText.startsWith("\n"))
      iText = iText.substring(1);

    OLogManager.instance().info(this, iText);
  }

  @Override
  public boolean isRequiredOpenDatabase() {
    return true;
  }

}
