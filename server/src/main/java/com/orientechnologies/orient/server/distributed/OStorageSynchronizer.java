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
package com.orientechnologies.orient.server.distributed;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.version.OVersionFactory;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog.DIRECTION;
import com.orientechnologies.orient.server.distributed.conflict.OReplicationConflictResolver;
import com.orientechnologies.orient.server.distributed.task.OReadRecordTask;
import com.orientechnologies.orient.server.journal.ODatabaseJournal;

/**
 * Manages replication across clustered nodes.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OStorageSynchronizer {
  private OServer                      server;
  private ODistributedServerManager    cluster;
  private ODatabaseJournal             log;
  private OReplicationConflictResolver resolver;

  public OStorageSynchronizer(final OServer iServer, final ODistributedServerManager iCluster, final String iStorageName)
      throws IOException {
    server = iServer;
    cluster = iCluster;
    final OStorage storage = openStorage(iStorageName);

    try {
      resolver = cluster.getConfictResolverClass().newInstance();
      resolver.startup(server, iCluster, iStorageName);
    } catch (Exception e) {
      ODistributedServerLog.error(this, cluster.getLocalNodeId(), null, DIRECTION.NONE,
          "cannot create the conflict resolver instance of class '%s'", cluster.getConfictResolverClass(), e);
    }

    final String logDirectory = OSystemVariableResolver.resolveSystemVariables(server.getDatabaseDirectory() + "/" + iStorageName);

    log = new ODatabaseJournal(iServer, cluster, storage, logDirectory);
  }

  public int recoverUncommited(final ODistributedServerManager iCluster, final String storageName) throws IOException {
    final OStorage storage = openStorage(storageName);

    ODistributedServerLog.info(this, iCluster.getLocalNodeId(), "*", DIRECTION.OUT, "recovering uncommitted operations...");

    int updated = 0;
    int deleted = 0;

    // RECOVER ALL THE UNCOMMITTED RECORDS ASKING TO THE CURRENT SERVERS FOR THEM
    try {
      for (Entry<ORecordId, Long> entry : log.getUncommittedOperations().entrySet()) {
        final ORecordId rid = entry.getKey();
        final long offset = entry.getValue();

        try {
          if (getConflictResolver().existConflictsForRecord(rid))
            // CONFLICT DETECTED, SKIPT IT
            continue;

          final OCluster recordCluster = storage.getClusterById(rid.getClusterId());
          if (recordCluster == null) {
            ODistributedServerLog.warn(this, iCluster.getLocalNodeId(), null, DIRECTION.NONE,
                "Cannot find cluster for RID %s, skip it", rid);
            continue;
          }

          final OReplicationConfig replicationData = iCluster.getReplicationData(storageName, recordCluster.getName(), rid,
              cluster.getLocalNodeId(), null);

          final ORawBuffer record = (ORawBuffer) iCluster.execute(getClusterNameByRID(storage, rid), rid, new OReadRecordTask(
              server, cluster, storageName, rid), replicationData);

          if (record == null) {
            // DELETE IT
            storage.deleteRecord(rid, OVersionFactory.instance().createUntrackedVersion(), 0, null);
            ODistributedServerLog.info(this, iCluster.getLocalNodeId(), "?", DIRECTION.IN, "restored record %s (delete)", rid);
            deleted++;
          } else {
            // UPDATE IT
            storage.updateRecord(rid, record.buffer, record.version, record.recordType, 0, null);
            ODistributedServerLog.info(this, iCluster.getLocalNodeId(), "?", DIRECTION.IN, "restored record %s (update)", rid);
            updated++;
          }

          // SET AS CANCELED
          log.setOperationStatus(offset, null, ODatabaseJournal.OPERATION_STATUS.CANCELED);

        } catch (ExecutionException e) {
          ODistributedServerLog
              .error(
                  this,
                  iCluster.getLocalNodeId(),
                  null,
                  DIRECTION.NONE,
                  "error on acquiring uncommitted record %s from other servers. The database could not be unaligned with others nodes!",
                  e, rid);
        }
      }

    } finally {
      ODistributedServerLog.info(this, iCluster.getLocalNodeId(), "*", DIRECTION.OUT,
          "recovered %d operations: updated=%d deleted=%d", (updated + deleted), updated, deleted);
    }

    return updated + deleted;
  }

  /**
   * Returns the conflict resolver implementation
   * 
   * @return
   */
  public OReplicationConflictResolver getConflictResolver() {
    return resolver;
  }

  public ODatabaseJournal getLog() {
    return log;
  }

  @Override
  public String toString() {
    return log != null ? log.getStorage().getName() : "<no-log>";
  }

  public static String getClusterNameByRID(final OStorage iStorage, final ORecordId iRid) {
    final OCluster cluster = iStorage.getClusterById(iRid.clusterId);
    return cluster != null ? cluster.getName() : "*";
  }

  protected OStorage openStorage(final String iName) {
    OStorage stg = Orient.instance().getStorage(iName);
    if (stg == null) {
      // NOT YET OPEN: OPEN IT NOW
      ODistributedServerLog.warn(this, cluster.getLocalNodeId(), null, DIRECTION.NONE, "Initializing storage '%s'...", iName);

      final String url = server.getStorageURL(iName);
      if (url == null)
        throw new IllegalArgumentException("Database '" + iName + "' is not configured on local server");
      stg = Orient.instance().loadStorage(url);
      stg.open(null, null, null);
    }
    return stg;
  }
}
