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
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.impl.ODistributedDatabaseChunk;
import com.orientechnologies.orient.server.distributed.task.ODistributedDatabaseDeltaSyncException;

import java.io.*;
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
  public static final int      FACTORYID            = 13;

  protected OLogSequenceNumber startLSN;
  protected Set<String>        excludedClusterNames = new HashSet<String>();

  public OSyncDatabaseDeltaTask() {
  }

  public OSyncDatabaseDeltaTask(final OLogSequenceNumber iFirstLSN, final long lastOperationTimestamp) {
    super(lastOperationTimestamp);
    this.startLSN = iFirstLSN;
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
          "Skip deploying database from the same node");

    return Boolean.FALSE;
  }

  public void excludeClusterName(final String name) {
    excludedClusterNames.add(name);
  }

  protected Object deltaBackup(final ODistributedRequestId requestId, final ODistributedServerManager iManager,
      final ODatabaseDocumentInternal database, final String databaseName) throws IOException, InterruptedException {

    final Long lastDeployment = (Long) iManager.getConfigurationMap().get(DEPLOYDB + databaseName);
    if (lastDeployment != null && lastDeployment.longValue() == random) {
      // SKIP IT
      ODistributedServerLog.debug(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.NONE,
          "Skip deploying delta database '%s' because already executed", databaseName);
      return Boolean.FALSE;
    }

    iManager.getConfigurationMap().put(DEPLOYDB + databaseName, random);

    final ODistributedDatabase dDatabase = checkIfCurrentDatabaseIsNotOlder(iManager, databaseName, startLSN);

    iManager.setDatabaseStatus(getNodeSource(), databaseName, ODistributedServerManager.DB_STATUS.SYNCHRONIZING);

    ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
        "Deploying database '%s' with delta of changes...", databaseName);

    // CREATE A BACKUP OF DATABASE
    final File backupFile = new File(Orient.getTempPath() + "/backup_" + getNodeSource() + "_" + database.getName() + ".zip");

    ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
        "Creating delta backup of database '%s' (startLSN=%s) in directory: %s...", databaseName, startLSN,
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
      endLSN.set(((OAbstractPaginatedStorage) storage).recordsChangedAfterLSN(startLSN, fileOutputStream, excludedClusterNames,
          new OCommandOutputListener() {
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
        exception.set(new ODistributedDatabaseDeltaSyncException(startLSN));
      } else
        ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
            "Delta backup of database '%s' completed. range=%s-%s", databaseName, startLSN, endLSN.get());

    } catch (Exception e) {
      // UNKNOWN ERROR, DELTA NOT AVAILABLE, TRY WITH FULL BACKUP
      exception.set(new ODistributedDatabaseDeltaSyncException(startLSN, e.getMessage()));

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

    ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
        "Deploy delta database task completed");

    // GET THE MOMENTUM, BUT OVERWRITE THE LAST LSN RECEIVED FROM THE DELTA
    final ODistributedMomentum momentum = dDatabase.getSyncConfiguration().getMomentum().copy();
    momentum.setLSN(iManager.getLocalNodeName(), endLSN.get());

    final ODistributedDatabaseChunk chunk = new ODistributedDatabaseChunk(backupFile, 0, CHUNK_MAX_SIZE, momentum, false);

    ODistributedServerLog.info(this, iManager.getLocalNodeName(), getNodeSource(), DIRECTION.OUT,
        "- transferring chunk #%d offset=%d size=%s...", 1, 0, OFileUtils.getSizeAsNumber(chunk.buffer.length));

    if (chunk.last)
      // NO MORE CHUNKS: SET THE NODE ONLINE (SYNCHRONIZING ENDED)
      iManager.setDatabaseStatus(iManager.getLocalNodeName(), databaseName, ODistributedServerManager.DB_STATUS.ONLINE);

    return chunk;
  }

  @Override
  public String getName() {
    return "deploy_delta_db";
  }

  @Override
  public void toStream(final DataOutput out) throws IOException {
    startLSN.toStream(out);
    out.writeLong(lastOperationTimestamp);
    out.writeLong(random);
    out.writeInt(excludedClusterNames.size());
    for (String clName : excludedClusterNames) {
      out.writeUTF(clName);
    }
  }

  @Override
  public void fromStream(final DataInput in, final ORemoteTaskFactory factory) throws IOException {
    startLSN = new OLogSequenceNumber(in);
    lastOperationTimestamp = in.readLong();
    random = in.readLong();
    excludedClusterNames.clear();
    final int total = in.readInt();
    for (int i = 0; i < total; ++i) {
      excludedClusterNames.add(in.readUTF());
    }
  }

  @Override
  public int getFactoryId() {
    return FACTORYID;
  }
}
