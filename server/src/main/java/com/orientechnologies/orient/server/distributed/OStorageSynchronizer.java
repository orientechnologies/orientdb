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

import java.io.File;
import java.io.IOException;
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
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.ODistributedTask.OPERATION;

/**
 * Manages replication across clustered nodes.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OStorageSynchronizer {
  private ODistributedServerManager        cluster;
  private String                           storageName;
  private OStorage                         storage;

  private ODocument                        configuration = new ODocument();
  private Map<String, OSynchronizationLog> logs          = new HashMap<String, OSynchronizationLog>();

  public OStorageSynchronizer(final ODistributedServerManager iCluster, final String iStorageName) {
    cluster = iCluster;
    storageName = iStorageName;
    storage = openStorage(iStorageName);
    configuration = iCluster.getLocalDatabaseConfiguration(iStorageName);

    final File replicationDirectory = new File(
        OSystemVariableResolver.resolveSystemVariables(OSynchronizationLog.REPLICATION_DIRECTORY + "/" + iStorageName));
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
    final OSynchronizationLog log = getLog(iNodeId);
    for (int i = 0; log.needAlignment() && i < log.totalEntries(); ++i) {
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
        final OPERATION operation = OPERATION.values()[op.type];

        // SEND THE RECORD TO THE REMOTE NODE
        cluster.executeOperation(iNodeId, operation, storageName, rid, op.version, record, EXECUTION_MODE.SYNCHRONOUS);

        switch (operation) {
        case RECORD_CREATE:
          aligned[0]++;
          break;

        case RECORD_UPDATE:
          aligned[1]++;
          break;

        case RECORD_DELETE:
          aligned[2]++;
          break;
        }
      }

      try {
        log.alignedEntry(i);
      } catch (IOException e) {
        OLogManager.instance().error(this, "DISTRIBUTED -> error on reset log entry %d", e, i);
      }
    }

    return aligned;
  }

  public boolean distributeOperation(final TYPE iType, final ORecord<?> iRecord) {
    OPERATION operation = null;

    switch (iType) {
    // DETERMINE THE OPERATION TYPE
    case AFTER_CREATE:
      operation = OPERATION.RECORD_CREATE;
      break;
    case AFTER_UPDATE:
      operation = OPERATION.RECORD_UPDATE;
      break;
    case AFTER_DELETE:
      operation = OPERATION.RECORD_DELETE;
      break;
    }

    if (operation != null) {
      final ORecordId rid = (ORecordId) iRecord.getIdentity();

      final String clusterName = storage.getClusterById(rid.getClusterId()).getName();
      if (!canExecuteOperation(clusterName, operation, "out")) {
        // IGNORE THIS CLUSTER
        OLogManager
            .instance()
            .debug(
                this,
                "DISTRIBUTED -> skip sending operation %s against cluster '%s' to remote nodes because of the distributed configuration",
                operation, clusterName);
        return false;
      }

      final EXECUTION_MODE mode = getOperationMode(clusterName, operation);

      for (OSynchronizationLog localLog : logs.values()) {
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
        cluster.executeOperation(members, operation, storageName, rid, iRecord.getVersion(), new ORawBuffer(
            (ORecordInternal<?>) iRecord), mode);

        // TODO MANAGE CONFLICTS
        for (String member : members) {
          final OSynchronizationLog log = getLog(member);
          try {
            log.success();
          } catch (IOException e) {
            OLogManager.instance().error(this, "DISTRIBUTED -> Error on reset log file: %s", e, log);
          }
        }
      }
    }

    return false;
  }

  @SuppressWarnings("unchecked")
  private boolean canExecuteOperation(final String iClusterName, final OPERATION iOperation, final String iDirection) {
    synchronized (configuration) {
      final Map<String, Object> clusters = configuration.field("clusters");

      Map<String, Object> cfg = (Map<String, Object>) clusters.get(iClusterName);
      if (cfg == null)
        cfg = (Map<String, Object>) clusters.get("*");

      final Boolean replicationEnabled = (Boolean) cfg.get("synchronization");
      if (replicationEnabled != null && !replicationEnabled)
        return false;

      final Map<String, Object> operations = (Map<String, Object>) cfg.get("operations");
      if (operations == null)
        return true;

      final Map<String, Object> operation = (Map<String, Object>) operations.get(iOperation.toString().toLowerCase());
      if (operation == null)
        return true;

      final String direction = (String) operation.get("direction");
      return direction == null || direction.contains(iDirection);
    }
  }

  @SuppressWarnings("unchecked")
  private EXECUTION_MODE getOperationMode(final String iClusterName, final OPERATION iOperation) {
    synchronized (configuration) {
      final Map<String, Object> clusters = configuration.field("clusters");

      Map<String, Object> cfg = (Map<String, Object>) clusters.get(iClusterName);
      if (cfg == null)
        cfg = (Map<String, Object>) clusters.get("*");

      Map<String, Object> operations = (Map<String, Object>) cfg.get("operations");
      if (operations == null)
        return EXECUTION_MODE.SYNCHRONOUS;

      final Map<String, Object> operation = (Map<String, Object>) operations.get(iOperation.toString().toLowerCase());
      if (operation == null)
        return EXECUTION_MODE.SYNCHRONOUS;

      return EXECUTION_MODE.valueOf(((String) operation.get("mode")).toUpperCase());
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

  protected OSynchronizationLog getLog(final String iNodeId) {
    synchronized (logs) {
      OSynchronizationLog localLog = logs.get(iNodeId);
      if (localLog == null) {
        try {
          ODocument cfg = cluster.getLocalDatabaseConfiguration(storageName);

          Number limit = cfg.field("clusters['*'][offlineMaxBuffer]");
          if (limit == null)
            limit = (long) -1;

          localLog = new OSynchronizationLog(cluster, iNodeId, storageName, limit.longValue());
          logs.put(iNodeId, localLog);
        } catch (IOException e) {
          OLogManager.instance().error("DISTRIBUTED -> Error on creation replication log for storage '%s' node '%s'", storageName,
              iNodeId);
        }
      }
      return localLog;
    }
  }

  protected OPERATION getOperation(final byte iRecordOperation) {
    if (iRecordOperation == ORecordOperation.CREATED)
      return OPERATION.RECORD_CREATE;
    else if (iRecordOperation == ORecordOperation.UPDATED)
      return OPERATION.RECORD_UPDATE;
    else if (iRecordOperation == ORecordOperation.DELETED)
      return OPERATION.RECORD_DELETE;
    throw new IllegalArgumentException("Illegal operation read from log: " + iRecordOperation);
  }

  @Override
  public String toString() {
    return storageName;
  }
}
