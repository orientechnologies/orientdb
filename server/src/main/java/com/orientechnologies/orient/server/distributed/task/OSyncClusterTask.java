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
import com.orientechnologies.orient.core.compression.impl.OZIPCompressionUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OClusterPositionMap;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OPaginatedCluster;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedDatabaseChunk;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import java.util.concurrent.locks.Lock;

/**
 * Ask for deployment of single cluster from a remote node.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSyncClusterTask extends OAbstractReplicatedTask {
  public final static int    CHUNK_MAX_SIZE = 4194304;         // 4MB
  public static final String DEPLOYCLUSTER  = "deploycluster.";

  public enum MODE {
    FULL_REPLACE, MERGE
  }

  protected MODE   mode = MODE.FULL_REPLACE;
  protected long   random;
  protected String clusterName;

  public OSyncClusterTask() {
  }

  public OSyncClusterTask(final String iClusterName) {
    random = UUID.randomUUID().getLeastSignificantBits();
    clusterName = iClusterName;
  }

  @Override
  public Object execute(final OServer iServer, final ODistributedServerManager iManager, final ODatabaseDocumentTx database)
      throws Exception {

    if (!getNodeSource().equals(iManager.getLocalNodeName())) {
      if (database == null)
        throw new ODistributedException("Database instance is null");

      final String databaseName = database.getName();

      final Lock lock = iManager.getLock("sync." + databaseName + "." + clusterName);
      if (lock.tryLock()) {
        try {

          final Long lastDeployment = (Long) iManager.getConfigurationMap().get(DEPLOYCLUSTER + databaseName + "." + clusterName);
          if (lastDeployment != null && lastDeployment.longValue() == random) {
            // SKIP IT
            ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.NONE,
                "skip deploying cluster '%s' because already executed", clusterName);
            return Boolean.FALSE;
          }

          iManager.getConfigurationMap().put(DEPLOYCLUSTER + databaseName + "." + clusterName, random);

          ODistributedServerLog.warn(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT, "deploying cluster %s...",
              databaseName);

          final File backupFile = new File(Orient.getTempPath() + "/backup_" + database.getName() + "_" + clusterName + ".zip");
          if (backupFile.exists())
            backupFile.delete();
          else
            backupFile.getParentFile().mkdirs();
          backupFile.createNewFile();

          ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
              "creating backup of cluster '%s' in directory: %s...", databaseName, backupFile.getAbsolutePath());

          final OPaginatedCluster cluster = (OPaginatedCluster) database.getStorage().getClusterByName(clusterName);

          switch (mode) {
          case MERGE:
            throw new IllegalArgumentException("merge mode not supported");

          case FULL_REPLACE:
            final FileOutputStream fileOutputStream = new FileOutputStream(backupFile);

            final File completedFile = new File(backupFile.getAbsolutePath() + ".completed");
            if (completedFile.exists())
              completedFile.delete();

            new Thread(new Runnable() {
              @Override
              public void run() {
                Thread.currentThread().setName(
                    "OrientDB SyncCluster node=" + iManager.getLocalNodeName() + " db=" + databaseName + " cluster=" + clusterName);

                try {
                  database.activateOnCurrentThread();
                  database.freeze();
                  try {

                    final String fileName = cluster.getFileName();

                    final String dbPath = iServer.getDatabaseDirectory() + databaseName;

                    final String[] fileNames = new String[] { fileName,
                        fileName.substring(0, fileName.length() - 4) + OClusterPositionMap.DEF_EXTENSION };

                    // COPY PCL AND CPM FILE
                    OZIPCompressionUtil.compressFiles(dbPath, fileNames, fileOutputStream, null,
                        OGlobalConfiguration.DISTRIBUTED_DEPLOYDB_TASK_COMPRESSION.getValueAsInteger());

                  } catch (IOException e) {
                    OLogManager.instance().error(this, "Cannot execute backup of cluster '%s.%s' for deploy cluster", e,
                        databaseName, clusterName);
                  } finally {
                    database.release();
                  }
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

            // TODO: SUPPORT BACKUP ON CLUSTER
            final long fileSize = backupFile.length();

            ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
                "sending the compressed cluster '%s.%s' over the NETWORK to node '%s', size=%s...", databaseName, clusterName,
                getNodeSource(), OFileUtils.getSizeAsString(fileSize));

            final ODistributedDatabaseChunk chunk = new ODistributedDatabaseChunk(0, backupFile, 0, CHUNK_MAX_SIZE);

            ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
                "- transferring chunk #%d offset=%d size=%s...", 1, 0, OFileUtils.getSizeAsNumber(chunk.buffer.length));

            return chunk;
          }

        } finally {
          lock.unlock();

          ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), ODistributedServerLog.DIRECTION.OUT,
              "deploy cluster %s task completed", clusterName);
        }
      }
    } else
      ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.NONE,
          "skip deploying cluster %s.%s from the same node");

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
    return false;
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
    return "deploy_cluster";
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    out.writeLong(random);
    out.writeUTF(clusterName);
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    random = in.readLong();
    clusterName = in.readUTF();
  }

  @Override
  public boolean isRequiredOpenDatabase() {
    return true;
  }

}
