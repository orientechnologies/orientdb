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
import com.orientechnologies.common.util.OUncaughtExceptionHandler;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.compression.impl.OZIPCompressionUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ORemoteTaskFactory;
import com.orientechnologies.orient.server.distributed.impl.ODistributedDatabaseChunk;
import com.orientechnologies.orient.server.distributed.task.OAbstractRemoteTask;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask.RESULT_STRATEGY;
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Ask for deployment of single cluster from a remote node.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class OSyncClusterTask extends OAbstractRemoteTask {
  public static final int CHUNK_MAX_SIZE = 4194304; // 4MB
  public static final String DEPLOYCLUSTER = "deploycluster.";
  public static final int FACTORYID = 12;

  public enum MODE {
    FULL_REPLACE,
    MERGE
  }

  protected MODE mode = MODE.FULL_REPLACE;
  protected long random;
  protected String clusterName;

  public OSyncClusterTask() {}

  public OSyncClusterTask(final String iClusterName) {
    random = UUID.randomUUID().getLeastSignificantBits();
    clusterName = iClusterName;
  }

  @Override
  public Object execute(
      ODistributedRequestId requestId,
      final OServer iServer,
      final ODistributedServerManager iManager,
      final ODatabaseDocumentInternal database)
      throws Exception {

    if (getNodeSource() == null || !getNodeSource().equals(iManager.getLocalNodeName())) {
      if (database == null) throw new ODistributedException("Database instance is null");

      final String databaseName = database.getName();

      try {

        ODistributedServerLog.info(
            this,
            iManager.getLocalNodeName(),
            getNodeSource(),
            DIRECTION.OUT,
            "deploying cluster %s...",
            databaseName);

        final File backupFile =
            new File(Orient.getTempPath() + "/backup_" + databaseName + "_" + clusterName + ".zip");
        if (backupFile.exists()) backupFile.delete();
        else backupFile.getParentFile().mkdirs();
        backupFile.createNewFile();

        ODistributedServerLog.info(
            this,
            iManager.getLocalNodeName(),
            getNodeSource(),
            DIRECTION.OUT,
            "Creating backup of cluster '%s' in directory: %s...",
            databaseName,
            backupFile.getAbsolutePath());

        switch (mode) {
          case MERGE:
            throw new IllegalArgumentException("Merge mode not supported");

          case FULL_REPLACE:
            final FileOutputStream fileOutputStream = new FileOutputStream(backupFile);

            final File completedFile = new File(backupFile.getAbsolutePath() + ".completed");
            if (completedFile.exists()) completedFile.delete();

            Thread t =
                new Thread(
                    new Runnable() {
                      @Override
                      public void run() {
                        Thread.currentThread()
                            .setName(
                                "OrientDB SyncCluster node="
                                    + iManager.getLocalNodeName()
                                    + " db="
                                    + databaseName
                                    + " cluster="
                                    + clusterName);

                        try {
                          database.activateOnCurrentThread();
                          database.freeze();

                          try {
                            final String dbPath = iServer.getDatabaseDirectory() + databaseName;

                            final Map<String, String> fileNames = new LinkedHashMap<>();

                            final OAbstractPaginatedStorage paginatedStorage =
                                (OAbstractPaginatedStorage) database.getStorage();
                            final OWriteCache writeCache = paginatedStorage.getWriteCache();

                            final OutputStream outputStream =
                                new BufferedOutputStream(fileOutputStream, CHUNK_MAX_SIZE);
                            try {
                              OZIPCompressionUtil.compressFiles(
                                  dbPath,
                                  fileNames,
                                  outputStream,
                                  null,
                                  OGlobalConfiguration.DISTRIBUTED_DEPLOYDB_TASK_COMPRESSION
                                      .getValueAsInteger());
                            } finally {
                              outputStream.flush();
                              outputStream.close();
                            }
                          } catch (IOException e) {
                            OLogManager.instance()
                                .error(
                                    this,
                                    "Cannot execute backup of cluster '%s.%s' for deploy cluster",
                                    e,
                                    databaseName,
                                    clusterName);
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
                            OLogManager.instance()
                                .error(
                                    this,
                                    "Cannot create file of backup completed: %s",
                                    e,
                                    completedFile);
                          }
                        }
                      }
                    });

            t.setUncaughtExceptionHandler(new OUncaughtExceptionHandler());
            t.start();

            // TODO: SUPPORT BACKUP ON CLUSTER
            final long fileSize = backupFile.length();

            ODistributedServerLog.info(
                this,
                iManager.getLocalNodeName(),
                getNodeSource(),
                DIRECTION.OUT,
                "Sending the compressed cluster '%s.%s' over the NETWORK to node '%s', size=%s...",
                databaseName,
                clusterName,
                getNodeSource(),
                OFileUtils.getSizeAsString(fileSize));

            final ODistributedDatabaseChunk chunk =
                new ODistributedDatabaseChunk(backupFile, 0, CHUNK_MAX_SIZE, false, false);

            ODistributedServerLog.info(
                this,
                iManager.getLocalNodeName(),
                getNodeSource(),
                DIRECTION.OUT,
                "- transferring chunk #%d offset=%d size=%s...",
                1,
                0,
                OFileUtils.getSizeAsNumber(chunk.buffer.length));

            return chunk;
        }

      } catch (OLockException e) {
        ODistributedServerLog.debug(
            this,
            iManager.getLocalNodeName(),
            getNodeSource(),
            DIRECTION.NONE,
            "Skip deploying cluster %s.%s because another node is doing it",
            databaseName,
            clusterName);
      } finally {
        ODistributedServerLog.info(
            this,
            iManager.getLocalNodeName(),
            getNodeSource(),
            ODistributedServerLog.DIRECTION.OUT,
            "Deploy cluster %s task completed",
            clusterName);
      }
    } else
      ODistributedServerLog.debug(
          this,
          iManager.getLocalNodeName(),
          getNodeSource(),
          DIRECTION.NONE,
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

  private static void addFileById(
      Map<String, String> entries, long fileId, OWriteCache writeCache) {
    final String nativeFileName = writeCache.nativeFileNameById(fileId);
    if (nativeFileName == null)
      throw new IllegalStateException("unable to resolve native file name of `" + fileId + "`");

    final String fileName = writeCache.fileNameById(fileId);
    if (fileName == null)
      throw new IllegalStateException("unable to resolve file name of `" + fileId + "`");

    entries.put(nativeFileName, fileName);
  }

  private static void addFileByName(
      Map<String, String> entries, String fileName, OWriteCache writeCache) {
    final long fileId = writeCache.fileIdByName(fileName);
    if (fileId == -1)
      throw new IllegalStateException("unable to resolve file id of `" + fileName + "`");

    final String nativeFileName = writeCache.nativeFileNameById(fileId);
    if (nativeFileName == null)
      throw new IllegalStateException("unable to resolve native file name of `" + fileName + "`");

    entries.put(nativeFileName, fileName);
  }
}
