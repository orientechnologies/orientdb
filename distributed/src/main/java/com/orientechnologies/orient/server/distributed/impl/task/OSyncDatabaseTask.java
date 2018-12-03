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
import com.orientechnologies.common.util.OUncaughtExceptionHandler;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.impl.ODistributedDatabaseChunk;
import com.orientechnologies.orient.server.distributed.impl.ODistributedStorage;
import com.sun.jna.platform.FileUtils;

import java.io.*;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Ask for synchronization of database from a remote node.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class OSyncDatabaseTask extends OAbstractSyncDatabaseTask {
  public static final int FACTORYID = 14;

  public OSyncDatabaseTask() {
  }

  public OSyncDatabaseTask(final OLogSequenceNumber lastLSN, final long lastOperationTimestamp) {
    super(lastOperationTimestamp);
    this.lastLSN = lastLSN;
  }

  @Override
  public Object execute(final ODistributedRequestId requestId, final OServer iServer, final ODistributedServerManager iManager,
      final ODatabaseDocumentInternal database) throws Exception {

    if (!iManager.getLocalNodeName().equals(getNodeSource())) {
      if (database == null)
        throw new ODistributedException("Database instance is null");

      final String databaseName = database.getName();

      final ODistributedDatabase dDatabase = checkIfCurrentDatabaseIsNotOlder(iManager, databaseName, database);

      try {
        final Long lastDeployment = (Long) iManager.getConfigurationMap().get(DEPLOYDB + databaseName);
        if (lastDeployment != null && lastDeployment.longValue() == random) {
          // SKIP IT
          ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.NONE,
              "Skip deploying database '%s' because already executed", databaseName);
          return Boolean.FALSE;
        }

        iManager.getConfigurationMap().put(DEPLOYDB + databaseName, random);

        iManager.setDatabaseStatus(getNodeSource(), databaseName, ODistributedServerManager.DB_STATUS.SYNCHRONIZING);

        ODistributedServerLog
            .info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT, "Deploying database %s...", databaseName);

        final AtomicReference<ODistributedMomentum> momentum = new AtomicReference<ODistributedMomentum>();

        OBackgroundBackup backup = ((ODistributedStorage) database.getStorage()).getLastValidBackup();
        if (backup == null || !backup.getResultedBackupFile().exists()) {
          // CREATE A BACKUP OF DATABASE FROM SCRATCH
          File backupFile = new File(Orient.getTempPath() + "/backup_" + database.getName() + ".zip");
          String backupPath = backupFile.getAbsolutePath();

          final int compressionRate = OGlobalConfiguration.DISTRIBUTED_DEPLOYDB_TASK_COMPRESSION.getValueAsInteger();

          if (backupFile.exists()) {
            if (backupFile.isDirectory()) {
              OFileUtils.deleteRecursively(backupFile);
            }
            backupFile.delete();
          } else
            backupFile.getParentFile().mkdirs();
          backupFile.createNewFile();

          final File resultedBackupFile = backupFile;

          final File completedFile = new File(backupFile.getAbsolutePath() + ".completed");
          if (completedFile.exists())
            completedFile.delete();

          ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
              "Creating backup of database '%s' (compressionRate=%d) in directory: %s...", databaseName, compressionRate,
              backupPath);

          backup = new OBackgroundBackup(this, iManager, database, resultedBackupFile, backupPath, null, momentum, dDatabase,
              requestId, completedFile);
          Thread t = new Thread(backup);
          t.setUncaughtExceptionHandler(new OUncaughtExceptionHandler());
          t.start();

          // RECORD LAST BACKUP TO BE REUSED IN CASE ANOTHER NODE ASK FOR THE SAME IN SHORT TIME WHILE THE DB IS NOT UPDATED
          ((ODistributedStorage) database.getStorage()).setLastValidBackup(backup);

        } else {
          momentum.set(dDatabase.getSyncConfiguration().getMomentum().copy());
          ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
              "Reusing last backup of database '%s' in directory: %s...", databaseName,
              backup.getResultedBackupFile().getAbsolutePath());
        }

        for (int retry = 0; momentum.get() == null && retry < 10; ++retry)
          Thread.sleep(300);

        backup.getStarted().await(1, TimeUnit.MINUTES);

        File backupFile = new File(backup.getFinalBackupPath());
        if (backup.getIncremental().get()) {
          iManager.setDatabaseStatus(getNodeSource(), databaseName, ODistributedServerManager.DB_STATUS.ONLINE);
          backupFile = backupFile.listFiles(pathname -> pathname.getName().endsWith(".ibu"))[0];
        }

        final ODistributedDatabaseChunk chunk = new ODistributedDatabaseChunk(backupFile, 0, CHUNK_MAX_SIZE, momentum.get(), false,
            backup.getIncremental().get());

        ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), ODistributedServerLog.DIRECTION.OUT,
            "- transferring chunk #%d offset=%d size=%s lsn=%s...", 1, 0, OFileUtils.getSizeAsNumber(chunk.buffer.length),
            momentum.get());

        if (chunk.last) {
          // NO MORE CHUNKS: SET THE NODE ONLINE (SYNCHRONIZING ENDED)
          iManager.setDatabaseStatus(iManager.getLocalNodeName(), databaseName, ODistributedServerManager.DB_STATUS.ONLINE);
          if (backup.getIncremental().get()) {
            File dir = backupFile.getParentFile();
            Arrays.stream(dir.listFiles()).forEach(x -> x.delete());
            dir.delete();
          }

        }

        return chunk;

      } catch (OLockException e) {
        ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.NONE,
            "Skip deploying database %s because another node is doing it", databaseName);
      } finally {
        ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), ODistributedServerLog.DIRECTION.OUT,
            "Deploy database task completed");
      }
    } else
      ODistributedServerLog.debug(this, iManager.getLocalNodeName(),

          getNodeSource(), DIRECTION.NONE, "Skip deploying database from the same node");

    return Boolean.FALSE;
  }

  protected ODistributedDatabase checkIfCurrentDatabaseIsNotOlder(final ODistributedServerManager iManager,
      final String databaseName, ODatabaseDocumentInternal database) {
    final ODistributedDatabase dDatabase = iManager.getMessageService().getDatabase(databaseName);

    if (lastLSN != null) {
      final OLogSequenceNumber currentLSN = ((OAbstractPaginatedStorage) database.getStorage().getUnderlying()).getLSN();
      if (currentLSN != null) {
        // LOCAL AND REMOTE LSN PRESENT
        if (lastLSN.compareTo(currentLSN) <= 0)
          // REQUESTED LSN IS <= LOCAL LSN
          return dDatabase;
      }
    }
    if (lastOperationTimestamp > -1) {
      if (lastOperationTimestamp <= dDatabase.getSyncConfiguration().getLastOperationTimestamp())
        // NO LSN, BUT LOCAL DATABASE HAS BEEN WRITTEN AFTER THE REQUESTER, STILL OK
        return dDatabase;
    } else
      // NO LSN, NO TIMESTAMP, C'MON, CAN'T BE NEWER THAN THIS
      return dDatabase;

    return databaseIsOld(iManager, databaseName, dDatabase);
  }

  @Override
  public String getName() {
    return "deploy_db";
  }

  @Override
  public void toStream(final DataOutput out) throws IOException {
    writeOptionalLSN(out);
    out.writeLong(random);
    out.writeLong(lastOperationTimestamp);
  }

  @Override
  public void fromStream(final DataInput in, final ORemoteTaskFactory factory) throws IOException {
    readOptionalLSN(in);
    random = in.readLong();
    lastOperationTimestamp = in.readLong();
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }

}
