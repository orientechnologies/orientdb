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
import java.util.Set;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager.EXECUTION_MODE;
import com.orientechnologies.orient.server.journal.ODatabaseJournal;
import com.orientechnologies.orient.server.task.OAbstractDistributedTask;
import com.orientechnologies.orient.server.task.OAbstractDistributedTask.STATUS;

/**
 * Manages replication across clustered nodes.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OStorageSynchronizer {
  private ODistributedServerManager cluster;
  private String                    storageName;
  private OStorage                  storage;

  private ODatabaseJournal          log;

  public OStorageSynchronizer(final ODistributedServerManager iCluster, final String iStorageName) throws IOException {
    cluster = iCluster;
    storageName = iStorageName;
    storage = openStorage(iStorageName);

    final String logDirectory = OSystemVariableResolver.resolveSystemVariables(OServerMain.server().getDatabaseDirectory() + "/"
        + iStorageName);

    log = new ODatabaseJournal(storage, logDirectory);
  }

  public void distributeOperation(final byte operation, final ORecordId rid, final OAbstractDistributedTask<?> iTask) {
    // CREATE THE RIGHT TASK
    final String clusterName = storage.getClusterById(rid.getClusterId()).getName();
    iTask.setMode(getOperationMode(clusterName, iTask.getName()));
    final Set<String> targetNodes = cluster.getRemoteNodeIdsBut(iTask.getNodeSource());

    if (!targetNodes.isEmpty()) {
      // RESET THE SOURCE TO AVOID LOOPS
      iTask.setNodeSource(cluster.getLocalNodeId());
      iTask.setStatus(STATUS.REMOTE_EXEC);

      cluster.sendOperation2Nodes(targetNodes, iTask);

      // TODO MANAGE CONFLICTS
      for (String member : targetNodes) {
      }
    }
  }

  public ODatabaseJournal getLog() {
    return log;
  }

  @Override
  public String toString() {
    return storageName;
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
}
