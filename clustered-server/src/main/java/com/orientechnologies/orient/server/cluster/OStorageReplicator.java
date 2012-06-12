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
package com.orientechnologies.orient.server.cluster;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.hook.ORecordHook.TYPE;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.cluster.hazelcast.OHazelcastReplicationTask;
import com.orientechnologies.orient.server.cluster.log.OReplicationLog;
import com.orientechnologies.orient.server.distributed.OServerCluster;

/**
 * Manages replication across clustered nodes.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OStorageReplicator {
  private OServerCluster               cluster;
  private String                       storageName;
  private OStorage                     storage;

  private ODocument                    configuration = new ODocument();
  private Map<String, OReplicationLog> logs          = new HashMap<String, OReplicationLog>();

  public OStorageReplicator(final OServerCluster iCluster, final String iStorageName) {
    cluster = iCluster;
    storageName = iStorageName;
    storage = openStorage(iStorageName);
    configuration = iCluster.getServerDatabaseConfiguration(iStorageName);

    final File replicationDirectory = new File(OSystemVariableResolver.resolveSystemVariables(OReplicationLog.REPLICATION_DIRECTORY
        + "/" + iStorageName));
    if (!replicationDirectory.exists())
      replicationDirectory.mkdirs();
    else {
      if (!replicationDirectory.isDirectory()) {
        OLogManager.instance().warn(this, "DISTRIBUTED log directory is a file instead of a directory! Delete and recreate it");
        replicationDirectory.delete();
        replicationDirectory.mkdirs();
      }
    }
  }

  public int[] align(final String iNodeId) {
    // ALIGN THE ALL THE COFNIGURED DATABASES
    final ORecordOperation op = new ORecordOperation();

    int[] aligned = new int[3];
    final OReplicationLog log = getLog(iNodeId);
    for (int i = 0; i < log.totalEntries(); ++i) {
      // GET THE <I> LOG ENTRY
      try {
        log.getEntry(i, op);
      } catch (IOException e) {
        OLogManager.instance().error(this, "DISTRIBUTED -> align failed for log entry %d", e, i);
        return aligned;
      }

      if (op.type < 0)
        // RESET: SKIP IT
        continue;

      final ORecordId rid = (ORecordId) op.record.getIdentity();

      // READ THE RECORD
      final ORawBuffer record = storage.readRecord(rid, null, false, null);
      if (record == null && op.type != ORecordOperation.DELETED)
        OLogManager.instance().warn(this, "DISTRIBUTED -> align failed for record %s because doesn't exist anymore", rid);
      else {
        // SEND THE RECORD TO THE REMOTE NODE
        cluster.executeOperation(iNodeId, op.type, storageName, rid, op.version, record);

        try {
          log.resetEntry(i);
        } catch (IOException e) {
          OLogManager.instance().error(this, "DISTRIBUTED -> error on reset log entry %d", e, i);
        }

        if (op.type == ORecordOperation.CREATED)
          aligned[0]++;
        else if (op.type == ORecordOperation.UPDATED)
          aligned[1]++;
        else if (op.type == ORecordOperation.DELETED)
          aligned[2]++;
      }
    }

    try {
      // FORCE RESET OF THE ENTIRE LOG
      log.reset();
    } catch (IOException e) {
      OLogManager.instance().error(this, "DISTRIBUTED -> Error on reset log file: %s", log, e);
    }

    return aligned;
  }

  public boolean distributeOperation(final TYPE iType, final ORecord<?> iRecord) {
    byte operation = -1;

    final ORecordId rid = (ORecordId) iRecord.getIdentity();

    switch (iType) {
    // DETERMINE THE OPERATION TYPE
    case AFTER_CREATE:
      operation = OHazelcastReplicationTask.CREATE;
      break;
    case AFTER_UPDATE:
      operation = OHazelcastReplicationTask.UPDATE;
      break;
    case AFTER_DELETE:
      operation = OHazelcastReplicationTask.DELETE;
      break;
    }

    if (operation > -1) {
      if (!isClusterReplicated(storage.getClusterById(rid.getClusterId()).getName()))
        // IGNORE THIS CLUSTER
        return false;

      for (OReplicationLog localLog : logs.values()) {
        // LOG THE OPERATION BEFORE ANY CHANGE
        try {
          if (localLog.appendLog(operation, rid, iRecord.getVersion()) == -1) {
            // LIMIT REACHED: PUT THE NODE OFFLINE
            OLogManager
                .instance()
                .warn(
                    this,
                    "DISTRIBUTED -> replication log limit reached for file: %s. The node will be offline and a manual alignment will be needed",
                    localLog);
          }
        } catch (IOException e) {
          OLogManager.instance()
              .error(this,
                  "DISTRIBUTED -> Error on appending log in file: %s. The coherence of the cluster is not more guaranteed", e,
                  localLog);
        }
      }

      final Set<String> members = cluster.getRemoteNodeIds();
      if (!members.isEmpty()) {
        final Collection<Object> results = cluster.executeOperation(members, operation, storageName, rid, iRecord.getVersion(),
            new ORawBuffer((ORecordInternal<?>) iRecord));

        // TODO MANAGE CONFLICTS
        for (String member : members) {
          final OReplicationLog log = getLog(member);
          try {
            log.resetIfEmpty();
          } catch (IOException e) {
            OLogManager.instance().error(this, "DISTRIBUTED -> Error on reset log file: %s", e, log);
          }
        }
      }
    }

    return false;
  }

  @SuppressWarnings("unchecked")
  private boolean isClusterReplicated(final String iClusterName) {
    synchronized (configuration) {
      final Map<String, Object> clusters = configuration.field("clusters");

      Map<String, Object> cfg = (Map<String, Object>) clusters.get(iClusterName);
      if (cfg == null)
        cfg = (Map<String, Object>) clusters.get("*");

      return (Boolean) cfg.get("replication");
    }
  }

  protected OStorage openStorage(final String iName) {
    OStorage stg = Orient.instance().getStorage(iName);
    if (stg == null) {
      // NOT YET OPEN: OPEN IT NOW
      OLogManager.instance().warn(this, "DISTRIBUTED Initializing storage '%s'", iName);

      stg = Orient.instance().loadStorage(OServerMain.server().getStorageURL(iName));
      stg.open(null, null, null);
    }
    return stg;
  }

  protected OReplicationLog getLog(final String iNodeId) {
    synchronized (logs) {
      OReplicationLog localLog = logs.get(iNodeId);
      if (localLog == null) {
        try {
          ODocument cfg = cluster.getServerDatabaseConfiguration(storageName);

          Number limit = cfg.field("clusters['*'][offlineMaxBuffer]");
          if (limit == null)
            limit = (long) -1;

          localLog = new OReplicationLog(iNodeId, storageName, limit.longValue());
          logs.put(iNodeId, localLog);
        } catch (IOException e) {
          OLogManager.instance().error("DISTRIBUTED -> Error on creation replication log for storage '%s' node '%s'", storageName,
              iNodeId);
        }
      }
      return localLog;
    }
  }

  @Override
  public String toString() {
    return storageName;
  }
}
