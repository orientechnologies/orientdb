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

import java.io.*;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.impl.ODistributedDatabaseChunk;
import com.orientechnologies.orient.server.distributed.task.OAbstractReplicatedTask;
import com.orientechnologies.orient.server.distributed.task.ODistributedDatabaseDeltaSyncException;

/**
 * Ask for synchronization of delta of chanegs on database from a remote node.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OSyncDatabaseDeltaTask extends OAbstractReplicatedTask {
  public final static int      CHUNK_MAX_SIZE       = 4194304;              // 4MB
  public static final String   DEPLOYDB             = "deploydb.";
  public static final int      FACTORYID            = 13;

  protected OLogSequenceNumber startLSN;
  protected long               random;
  protected Set<String>        excludedClusterNames = new HashSet<String>();

  public OSyncDatabaseDeltaTask() {
  }

  public OSyncDatabaseDeltaTask(final OLogSequenceNumber iFirstLSN) {
    startLSN = iFirstLSN;
    random = UUID.randomUUID().getLeastSignificantBits();
  }

  @Override
  public Object execute(final ODistributedRequestId requestId, final OServer iServer, final ODistributedServerManager iManager,
      final ODatabaseDocumentInternal database) throws Exception {

    if (!getNodeSource().equals(iManager.getLocalNodeName())) {
      if (database == null)
        throw new ODistributedException("Database instance is null");

      final String databaseName = database.getName();

      final Object chunk = deltaBackup(requestId, iManager, database, databaseName);
      if (chunk != null)
        return chunk;

    } else
      ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.NONE,
          "skip deploying database from the same node");

    return Boolean.FALSE;
  }

  public void excludeClusterName(final String name) {
    excludedClusterNames.add(name);
  }

  protected Object deltaBackup(final ODistributedRequestId requestId, final ODistributedServerManager iManager,
      final ODatabaseDocumentInternal database, final String databaseName) throws IOException, InterruptedException {

    try {

      final Long lastDeployment = (Long) iManager.getConfigurationMap().get(DEPLOYDB + databaseName);
      if (lastDeployment != null && lastDeployment.longValue() == random) {
        // SKIP IT
        ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.NONE,
            "skip deploying delta database '%s' because already executed", databaseName);
        return Boolean.FALSE;
      }

      iManager.getConfigurationMap().put(DEPLOYDB + databaseName, random);

      iManager.setDatabaseStatus(getNodeSource(), databaseName, ODistributedServerManager.DB_STATUS.SYNCHRONIZING);
      iManager.setDatabaseStatus(iManager.getLocalNodeName(), databaseName, ODistributedServerManager.DB_STATUS.SYNCHRONIZING);

      ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
          "deploying database %s with delta of changes...", databaseName);

      // CREATE A BACKUP OF DATABASE
      final File backupFile = new File(Orient.getTempPath() + "/backup_" + getNodeSource() + "_" + database.getName() + ".zip");

      ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
          "creating delta backup of database '%s' (startLSN=%s) in directory: %s...", databaseName, startLSN,
          backupFile.getAbsolutePath());

      if (backupFile.exists())
        backupFile.delete();
      else
        backupFile.getParentFile().mkdirs();
      backupFile.createNewFile();

      final FileOutputStream fileOutputStream = new FileOutputStream(backupFile);
      // final GZIPOutputStream gzipOutputStream = new GZIPOutputStream(fileOutputStream);

      final File completedFile = new File(backupFile.getAbsolutePath() + ".completed");
      if (completedFile.exists())
        completedFile.delete();

      final OStorage storage = database.getStorage().getUnderlying();
      if (!(storage instanceof OAbstractPaginatedStorage))
        throw new UnsupportedOperationException("Storage '" + storage.getName() + "' does not support distributed delta backup");

      final AtomicReference<OLogSequenceNumber> endLSN = new AtomicReference<OLogSequenceNumber>();
      final AtomicReference<ODistributedDatabaseDeltaSyncException> exception = new AtomicReference<ODistributedDatabaseDeltaSyncException>();

      try {
        endLSN.set(((OAbstractPaginatedStorage) storage).recordsChangedAfterLSN(startLSN, fileOutputStream, excludedClusterNames));

        if (endLSN.get() == null)
          // DELTA NOT AVAILABLE, TRY WITH FULL BACKUP
          exception.set(new ODistributedDatabaseDeltaSyncException(startLSN));

        ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
            "Delta backup of database '%s' completed. range=%s-%s...", databaseName, startLSN, endLSN);

      } catch (Exception e) {
        // UNKNOWN ERROR, DELTA NOT AVAILABLE, TRY WITH FULL BACKUP
        exception.set((ODistributedDatabaseDeltaSyncException) OException
            .wrapException(new ODistributedDatabaseDeltaSyncException(startLSN), e));

      } finally {
        // try {
        // gzipOutputStream.close();
        // } catch (IOException e) {
        // }

        try {
          fileOutputStream.close();
        } catch (IOException e) {
        }

        try {
          completedFile.createNewFile();
        } catch (IOException e) {
          OLogManager.instance().error(this, "Cannot create file of delta backup completed: %s", e, completedFile);
        }
      }

      if (exception.get() instanceof ODistributedDatabaseDeltaSyncException)
        throw exception.get();

      final ODistributedDatabaseChunk chunk = new ODistributedDatabaseChunk(backupFile, 0, CHUNK_MAX_SIZE, endLSN.get(), false);

      ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
          "- transferring chunk #%d offset=%d size=%s...", 1, 0, OFileUtils.getSizeAsNumber(chunk.buffer.length));

      if (chunk.last)
        // NO MORE CHUNKS: SET THE NODE ONLINE (SYNCHRONIZING ENDED)
        iManager.setDatabaseStatus(iManager.getLocalNodeName(), databaseName, ODistributedServerManager.DB_STATUS.ONLINE);

      return chunk;

    } finally {
      ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
          "deploy delta database task completed");
    }
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
    return "deploy_delta_db";
  }

  @Override
  public void writeExternal(final ObjectOutput out) throws IOException {
    startLSN.writeExternal(out);
    out.writeLong(random);
    out.writeInt(excludedClusterNames.size());
    for (String clName : excludedClusterNames) {
      out.writeUTF(clName);
    }
  }

  @Override
  public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
    startLSN = new OLogSequenceNumber(in);
    random = in.readLong();
    excludedClusterNames.clear();
    final int total = in.readInt();
    for (int i = 0; i < total; ++i) {
      excludedClusterNames.add(in.readUTF());
    }
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
