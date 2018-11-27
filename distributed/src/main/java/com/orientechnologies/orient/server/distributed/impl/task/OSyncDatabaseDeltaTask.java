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
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.disk.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedDatabase;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedMomentum;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.ORemoteTaskFactory;
import com.orientechnologies.orient.server.distributed.impl.ODistributedDatabaseChunk;
import com.orientechnologies.orient.server.distributed.task.ODistributedDatabaseDeltaSyncException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Ask for synchronization of delta of chanegs on database from a remote node.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class OSyncDatabaseDeltaTask extends OAbstractSyncDatabaseTask {
  public static final int FACTORYID = 13;

  protected Set<String> includeClusterNames = new HashSet<String>();

  public OSyncDatabaseDeltaTask() {
  }

  public OSyncDatabaseDeltaTask(final OLogSequenceNumber iFirstLSN, final long lastOperationTimestamp) {
    super(lastOperationTimestamp);
    this.lastLSN = iFirstLSN;
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
      ODistributedServerLog
          .debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.NONE, "Skip deploying database from the same node");

    return Boolean.FALSE;
  }

  public void includeClusterName(final String name) {
    includeClusterNames.add(name);
  }

  protected Object deltaBackup(final ODistributedRequestId requestId, final ODistributedServerManager iManager,
      final ODatabaseDocumentInternal database, final String databaseName) throws IOException, InterruptedException {

    final Long lastDeployment = (Long) iManager.getConfigurationMap().get(DEPLOYDB + databaseName);
    if (lastDeployment != null && lastDeployment.longValue() == random) {
      // SKIP IT
      ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.NONE,
          "Skip deploying of delta database '%s' because already executed", databaseName);
      return Boolean.FALSE;
    }

    iManager.getConfigurationMap().put(DEPLOYDB + databaseName, random);

    final ODistributedDatabase dDatabase = checkIfCurrentDatabaseIsNotOlder(iManager, databaseName, database);

    iManager.setDatabaseStatus(getNodeSource(), databaseName, ODistributedServerManager.DB_STATUS.SYNCHRONIZING);

    ODistributedServerLog
        .info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT, "Deploying database '%s' with delta of changes...",
            databaseName);

    // CREATE A BACKUP OF DATABASE
    final File backupFile = new File(
        Orient.getTempPath() + "/backup_" + getNodeSource() + "_" + database.getName() + "_server" + iManager.getLocalNodeId()
            + ".zip");

    ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
        "Creating delta backup of database '%s' (startLSN=%s) in directory: %s...", databaseName, lastLSN,
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
      final AtomicLong counter = new AtomicLong(0);
      endLSN.set(
          ((OAbstractPaginatedStorage) storage).recordsChangedAfterLSN(lastLSN, fileOutputStream, new OCommandOutputListener() {
            @Override
            public void onMessage(final String iText) {
              if (iText.startsWith("read")) {
                if (counter.incrementAndGet() % 100000 == 0) {
                  ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT, "- %s", iText);
                }
              } else if (counter.incrementAndGet() % 10000 == 0) {
                ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT, "- %s", iText);
              }
            }
          }));

      if (endLSN.get() == null) {
        // DELTA NOT AVAILABLE, TRY WITH FULL BACKUP
        exception.set(new ODistributedDatabaseDeltaSyncException(lastLSN));
      } else if (endLSN.get().equals(lastLSN)) {
        // nothing has changed
        return Boolean.FALSE;
      } else
        ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
            "Delta backup of database '%s' completed. range=%s-%s", databaseName, lastLSN, endLSN.get());

    } catch (Exception e) {
      // UNKNOWN ERROR, DELTA NOT AVAILABLE, TRY WITH FULL BACKUP
      exception.set(new ODistributedDatabaseDeltaSyncException(lastLSN, e.getMessage()));

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

    if (exception.get() instanceof ODistributedDatabaseDeltaSyncException) {
      throw exception.get();
    }

    ODistributedServerLog
        .info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT, "Deploy delta database task completed");

    // GET THE MOMENTUM, BUT OVERWRITE THE LAST LSN RECEIVED FROM THE DELTA
    final ODistributedMomentum momentum = dDatabase.getSyncConfiguration().getMomentum().copy();
    momentum.setLSN(iManager.getLocalNodeName(), endLSN.get());

    final ODistributedDatabaseChunk chunk = new ODistributedDatabaseChunk(backupFile, 0, CHUNK_MAX_SIZE, momentum, false, false);

    ODistributedServerLog
        .info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT, "- transferring chunk #%d offset=%d size=%s...", 1,
            0, OFileUtils.getSizeAsNumber(chunk.buffer.length));

    if (chunk.last)
      // NO MORE CHUNKS: SET THE NODE ONLINE (SYNCHRONIZING ENDED)
      iManager.setDatabaseStatus(iManager.getLocalNodeName(), databaseName, ODistributedServerManager.DB_STATUS.ONLINE);

    return chunk;
  }

  protected ODistributedDatabase checkIfCurrentDatabaseIsNotOlder(final ODistributedServerManager iManager,
      final String databaseName, ODatabaseDocumentInternal database) {
    final ODistributedDatabase dDatabase = iManager.getMessageService().getDatabase(databaseName);

    if (lastLSN != null) {
      final OLogSequenceNumber currentLSN = ((OLocalPaginatedStorage) database.getStorage().getUnderlying()).getLSN();
      if (currentLSN != null) {
        // LOCAL AND REMOTE LSN PRESENT
        if (lastLSN.compareTo(currentLSN) <= 0)
          // REQUESTED LSN IS <= LOCAL LSN
          return dDatabase;
        else
          throw new ODistributedDatabaseDeltaSyncException(lastLSN);
      }
    } else if (lastOperationTimestamp > -1) {
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
    return "deploy_delta_db";
  }

  @Override
  public void toStream(final DataOutput out) throws IOException {
    lastLSN.toStream(out);
    out.writeLong(lastOperationTimestamp);
    out.writeLong(random);
    out.writeInt(includeClusterNames.size());
    for (String clName : includeClusterNames) {
      out.writeUTF(clName);
    }
  }

  @Override
  public void fromStream(final DataInput in, final ORemoteTaskFactory factory) throws IOException {
    lastLSN = new OLogSequenceNumber(in);
    lastOperationTimestamp = in.readLong();
    random = in.readLong();
    includeClusterNames.clear();
    final int total = in.readInt();
    for (int i = 0; i < total; ++i) {
      includeClusterNames.add(in.readUTF());
    }
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }
}
