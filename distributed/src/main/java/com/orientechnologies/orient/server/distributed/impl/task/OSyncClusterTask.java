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

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCollections;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.compression.impl.OZIPCompressionUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sharding.auto.OAutoShardingIndexEngine;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OClusterPositionMap;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OPaginatedCluster;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.impl.ODistributedDatabaseChunk;
import com.orientechnologies.orient.server.distributed.task.OAbstractReplicatedTask;

import java.io.*;
import java.util.ArrayList;
import java.util.UUID;

/**
 * Ask for deployment of single cluster from a remote node.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class OSyncClusterTask extends OAbstractReplicatedTask {
  public final static int    CHUNK_MAX_SIZE = 4194304;         // 4MB
  public static final String DEPLOYCLUSTER  = "deploycluster.";
  public static final int    FACTORYID      = 12;

  public enum MODE {
    FULL_REPLACE, MERGE
  }

  protected MODE mode = MODE.FULL_REPLACE;
  protected long   random;
  protected String clusterName;

  public OSyncClusterTask() {
  }

  public OSyncClusterTask(final String iClusterName) {
    random = UUID.randomUUID().getLeastSignificantBits();
    clusterName = iClusterName;
  }

  @Override
  public Object execute(ODistributedRequestId requestId, final OServer iServer, final ODistributedServerManager iManager,
      final ODatabaseDocumentInternal database) throws Exception {

    if (getNodeSource() == null || !getNodeSource().equals(iManager.getLocalNodeName())) {
      if (database == null)
        throw new ODistributedException("Database instance is null");

      final String databaseName = database.getName();

      try {

        final Long lastDeployment = (Long) iManager.getConfigurationMap().get(DEPLOYCLUSTER + databaseName + "." + clusterName);
        if (lastDeployment != null && lastDeployment.longValue() == random) {
          // SKIP IT
          ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.NONE,
              "Skip deploying cluster '%s' because already executed", clusterName);
          return Boolean.FALSE;
        }

        iManager.getConfigurationMap().put(DEPLOYCLUSTER + databaseName + "." + clusterName, random);

        ODistributedServerLog
            .info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT, "deploying cluster %s...", databaseName);

        final File backupFile = new File(Orient.getTempPath() + "/backup_" + databaseName + "_" + clusterName + ".zip");
        if (backupFile.exists())
          backupFile.delete();
        else
          backupFile.getParentFile().mkdirs();
        backupFile.createNewFile();

        ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
            "Creating backup of cluster '%s' in directory: %s...", databaseName, backupFile.getAbsolutePath());

        final OPaginatedCluster cluster = (OPaginatedCluster) database.getStorage().getClusterByName(clusterName);

        switch (mode) {
        case MERGE:
          throw new IllegalArgumentException("Merge mode not supported");

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

                  final ArrayList<String> fileNames = new ArrayList<String>();
                  // COPY PCL AND CPM FILE
                  fileNames.add(fileName);
                  fileNames.add(fileName.substring(0, fileName.length() - 4) + OClusterPositionMap.DEF_EXTENSION);

                  final OClass clazz = database.getMetadata().getSchema().getClassByClusterId(cluster.getId());
                  if (clazz != null) {
                    // CHECK FOR AUTO-SHARDED INDEXES
                    final OIndex<?> asIndex = clazz.getAutoShardingIndex();
                    if (asIndex != null) {
                      final int partition = OCollections.indexOf(clazz.getClusterIds(), cluster.getId());
                      final String indexName = asIndex.getName();
                      fileNames.add(indexName + "_" + partition + OAutoShardingIndexEngine.SUBINDEX_METADATA_FILE_EXTENSION);
                      fileNames.add(indexName + "_" + partition + OAutoShardingIndexEngine.SUBINDEX_TREE_FILE_EXTENSION);
                      fileNames.add(indexName + "_" + partition + OAutoShardingIndexEngine.SUBINDEX_BUCKET_FILE_EXTENSION);
                      fileNames.add(indexName + "_" + partition + OAutoShardingIndexEngine.SUBINDEX_NULL_BUCKET_FILE_EXTENSION);
                    }
                  }

                  String[] fnArray = fileNames.toArray(new String[fileNames.size()]);

                  OZIPCompressionUtil.compressFiles(dbPath, fnArray, fileOutputStream, null, iServer.getContextConfiguration()
                    .getValueAsInteger(OGlobalConfiguration.DISTRIBUTED_DEPLOYDB_TASK_COMPRESSION));
                      

                } catch (IOException e) {
                  OLogManager.instance()
                      .error(this, "Cannot execute backup of cluster '%s.%s' for deploy cluster", e, databaseName, clusterName);
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
              "Sending the compressed cluster '%s.%s' over the NETWORK to node '%s', size=%s...", databaseName, clusterName,
              getNodeSource(), OFileUtils.getSizeAsString(fileSize));

          final ODistributedDatabaseChunk chunk = new ODistributedDatabaseChunk(backupFile, 0, CHUNK_MAX_SIZE, null, false);

          ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
              "- transferring chunk #%d offset=%d size=%s...", 1, 0, OFileUtils.getSizeAsNumber(chunk.buffer.length));

          return chunk;
        }

      } catch (OLockException e) {
        ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.NONE,
            "Skip deploying cluster %s.%s because another node is doing it", databaseName, clusterName);
      } finally {
        ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), ODistributedServerLog.DIRECTION.OUT,
            "Deploy cluster %s task completed", clusterName);
      }
    } else
      ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.NONE,
          "Skip deploying cluster %s.%s from the same node");

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
  public String getName() {
    return "deploy_cluster";
  }

  @Override
  public void toStream(final DataOutput out) throws IOException {
    out.writeLong(random);
    out.writeUTF(clusterName);
  }

  @Override
  public void fromStream(final DataInput in, final ORemoteTaskFactory factory) throws IOException {
    random = in.readLong();
    clusterName = in.readUTF();
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }

}
