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
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.EXECUTION_MODE;
import com.orientechnologies.orient.server.distributed.task.OAbstractDistributedTask;
import com.orientechnologies.orient.server.distributed.task.OCreateRecordDistributedTask;
import com.orientechnologies.orient.server.distributed.task.ODeleteRecordDistributedTask;
import com.orientechnologies.orient.server.distributed.task.OUpdateRecordDistributedTask;

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

  private Map<String, OSynchronizationLog> logs = new HashMap<String, OSynchronizationLog>();

  public OStorageSynchronizer(final ODistributedServerManager iCluster, final String iStorageName) {
    cluster = iCluster;
    storageName = iStorageName;
    storage = openStorage(iStorageName);

    final File replicationDirectory = new File(
        OSystemVariableResolver.resolveSystemVariables(OSynchronizationLog.SYNCHRONIZATION_DIRECTORY + "/" + iStorageName));
    if (!replicationDirectory.exists())
      replicationDirectory.mkdirs();
    else {
      if (!replicationDirectory.isDirectory()) {
        OLogManager.instance().warn(this, "DISTRIBUTED log directory is a file instead of a directory! Delete and recreate it");
        replicationDirectory.delete();
        replicationDirectory.mkdirs();
      } else {
        // OPEN ALL THE LOGS
        for (File sub : replicationDirectory.listFiles()) {
          OSynchronizationLog localLog;
          try {
            localLog = new OSynchronizationLog(cluster, sub, storageName, getLogLimit().longValue());
            logs.put(localLog.getNodeId(), localLog);
          } catch (IOException e) {
            OLogManager.instance().error(this, "Cannot open distributed log file: " + sub, e);
          }
        }
      }
    }
  }

  public int[] align(final String iNodeId) {
    // ALIGN THE ALL THE COFNIGURED DATABASES
    final ORecordOperation op = new ORecordOperation();

    final String currentNodeId = cluster.getLocalNodeId();

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

      boolean alignedOperation = false;

      final ORecordId rid = (ORecordId) op.record.getIdentity();

      if (rid.isNew())
        // IT'S THE LAST RECORD, GET IT FROM THE CLUSTER
        rid.clusterPosition = storage.getClusterDataRange(rid.clusterId)[1];

      // READ THE RECORD
      final ORawBuffer record = storage.readRecord(rid, null, false, null);
      if (record == null && op.type != ORecordOperation.DELETED)
        OLogManager.instance().warn(this, "DISTRIBUTED -> align failed for record %s because doesn't exist anymore", rid);
      else {
        final OAbstractDistributedTask<?> task = createTaskFromOperation(op.type, currentNodeId, EXECUTION_MODE.SYNCHRONOUS, rid,
            record);

        // SEND THE RECORD TO THE REMOTE NODE
        final Object result = cluster.sendOperation2Node(iNodeId, task);

        // TODO: CHECK CONFLICTS
        switch (op.type) {
        case ORecordOperation.CREATED:
          if (op.record.getIdentity().getClusterPosition() != ((OPhysicalPosition) result).clusterPosition)
            OLogManager.instance().warn(this,
                "DISTRIBUTED -> detected conflict on aligning journaled operation #%d %s RID local %s != remote #%d:%d", i,
                ORecordOperation.getName(op.type), rid, rid.getClusterId(), ((OPhysicalPosition) result).clusterPosition);
          else {
            alignedOperation = true;
            aligned[0]++;
          }
          break;

        case ORecordOperation.UPDATED:
          if (op.record.getRecord().getVersion() != ((Integer) result))
            OLogManager.instance().warn(this,
                "DISTRIBUTED -> detected conflict on aligning journaled operation #%d %s record %s VERSION local %d != remote %d",
                i, ORecordOperation.getName(op.type), rid, op.record.getRecord().getVersion(), result);
          else {
            alignedOperation = true;
            aligned[1]++;
          }
          break;

        case ORecordOperation.DELETED:
          alignedOperation = true;
          aligned[2]++;
          break;
        }
      }

      if (!alignedOperation) {
        OLogManager.instance().error(this, "DISTRIBUTED -> error on alignment: databases are not synchronized");
        break;
      }

      try {
        log.alignedEntry(i);
      } catch (IOException e) {
        OLogManager.instance().error(this, "DISTRIBUTED -> error on reset log entry %d", e, i);
      }
    }

    return aligned;
  }

  public void logOperation(final byte operation, final ORecordId rid, final int iVersion, final String iNodeSource) {
    // WRITE TO ALL THE LOGS BUT THE SOURCE NODE
    for (OSynchronizationLog localLog : logs.values()) {
      if (!localLog.getNodeId().equals(iNodeSource))
        try {
          if (localLog.appendLog(operation, rid, iVersion) == -1) {
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
  }

  public void updateOperationRid(final ORecordId iRid, final int iVersion, final String iNodeSource) {
    for (OSynchronizationLog localLog : logs.values()) {
      // UDPATE THE LAST LOG ENTRY
      if (!localLog.getNodeId().equals(iNodeSource))
        try {
          localLog.updateLog(iRid, iVersion);
        } catch (IOException e) {
          OLogManager.instance().error(this,
              "DISTRIBUTED -> Error on updating log in file: %s. The coherence of the cluster is not more guaranteed", e, localLog);
        }
    }
  }

  public void distributeOperation(final byte operation, final ORecordId rid, final OAbstractDistributedTask<?> iTask) {
    // CREATE THE RIGHT TASK
    final String clusterName = storage.getClusterById(rid.getClusterId()).getName();
    iTask.setMode(getOperationMode(clusterName, iTask.getName()));
    final Set<String> targetNodes = cluster.getRemoteNodeIdsBut(iTask.getNodeSource());

    if (!targetNodes.isEmpty()) {
      // RESET THE SOURCE TO AVOID LOOPS
      iTask.setNodeSource(cluster.getLocalNodeId());
      iTask.setRedistribute(false);

      try {
        cluster.sendOperation2Nodes(targetNodes, iTask);

        // TODO MANAGE CONFLICTS
        for (String member : targetNodes) {
          final OSynchronizationLog log = getLog(member);
          try {
            log.success();
          } catch (IOException e) {
            OLogManager.instance().error(this, "DISTRIBUTED -> Error on reset log file: %s", e, log);
          }
        }
      } catch (ODistributedException e) {
        // IGNORE IT BECAUSE THE LOG
        OLogManager.instance().error(this, "DISTRIBUTED -> Error on distributing operation, start buffering changes", e);
      }
    }
  }

  private EXECUTION_MODE getOperationMode(final String iClusterName, final String iOperation) {
    final ODocument clusters = cluster.getDatabaseConfiguration(storageName).field("clusters");

    if (clusters == null)
      return EXECUTION_MODE.SYNCHRONOUS;

    ODocument cfg = clusters.field(iClusterName);
    if (cfg == null)
      cfg = clusters.field("*");

    ODocument operations = cfg.field("operations");
    if (operations == null)
      return EXECUTION_MODE.SYNCHRONOUS;

    final ODocument operation = operations.field(iOperation.toLowerCase());
    if (operation == null)
      return EXECUTION_MODE.SYNCHRONOUS;

    final String mode = operation.field("mode");
    if (mode == null)
      return EXECUTION_MODE.SYNCHRONOUS;

    return EXECUTION_MODE.valueOf(((String) mode).toUpperCase());
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
          Number limit = getLogLimit();

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

  protected Number getLogLimit() {
    // ODocument cfg = cluster.getDatabaseConfiguration(storageName);
    //
    // Number limit = cfg.field("clusters['*'][offlineMaxBuffer]");
    // if (limit == null)
    // limit = (long) -1;
    // return limit;
    return 1000000;
  }

  @Override
  public String toString() {
    return storageName;
  }

  protected OAbstractDistributedTask<?> createTaskFromOperation(final byte iOperation, final String currentNodeId,
      final EXECUTION_MODE iMode, final ORecordId rid, final ORawBuffer record) {
    // CREATE THE RIGHT TASK

    final OAbstractDistributedTask<?> task;

    switch (iOperation) {
    case ORecordOperation.CREATED:
      task = new OCreateRecordDistributedTask(currentNodeId, storageName, iMode, rid, record.buffer, record.version,
          record.recordType);
      break;

    case ORecordOperation.UPDATED:
      task = new OUpdateRecordDistributedTask(currentNodeId, storageName, iMode, rid, record.buffer, record.version,
          record.recordType);
      break;

    case ORecordOperation.DELETED:
      task = new ODeleteRecordDistributedTask(currentNodeId, storageName, iMode, rid, record.version);
      break;

    default:
      throw new ODistributedException("Error on creating distributed task: Found not supported operation with code " + iOperation);
    }

    return task.setRedistribute(false);
  }
}
