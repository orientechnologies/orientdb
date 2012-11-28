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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.version.OVersionFactory;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.distributed.conflict.OReplicationConflictResolver;
import com.orientechnologies.orient.server.journal.ODatabaseJournal;
import com.orientechnologies.orient.server.task.OAbstractDistributedTask;
import com.orientechnologies.orient.server.task.OAbstractDistributedTask.STATUS;
import com.orientechnologies.orient.server.task.OReadRecordDistributedTask;

/**
 * Manages replication across clustered nodes.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OStorageSynchronizer {
  private ODistributedServerManager    cluster;
  private String                       storageName;
  private ODatabaseJournal             log;
  private OReplicationConflictResolver resolver;

  public OStorageSynchronizer(final ODistributedServerManager iCluster, final String storageName) throws IOException {
    cluster = iCluster;
    final OStorage storage = openStorage(storageName);

    try {
      resolver = iCluster.getConfictResolverClass().newInstance();
      resolver.startup(iCluster, storageName);
    } catch (Exception e) {
      OLogManager.instance().error(this, "Cannot create the conflict resolver instance of class '%s'",
          iCluster.getConfictResolverClass(), e);
    }

    final String logDirectory = OSystemVariableResolver.resolveSystemVariables(OServerMain.server().getDatabaseDirectory() + "/"
        + storageName);

    log = new ODatabaseJournal(storage, logDirectory);

    // RECOVER ALL THE UNCOMMITTED RECORDS ASKING TO THE CURRENT SERVERS FOR THEM
    for (ORecordId rid : log.getUncommittedOperations()) {
      try {
        if (getConflictResolver().existConflictsForRecord(rid))
          continue;

        final ORawBuffer record = (ORawBuffer) iCluster.routeOperation2Node(getClusterNameByRID(storage, rid), rid,
            new OReadRecordDistributedTask(iCluster.getLocalNodeId(), storageName, rid));

        if (record == null)
          // DELETE IT
          storage.deleteRecord(rid, OVersionFactory.instance().createUntrackedVersion(), 0, null);
        else
          // UPDATE IT
          storage.updateRecord(rid, record.buffer, record.version, record.recordType, 0, null);

      } catch (ExecutionException e) {
        OLogManager
            .instance()
            .warn(
                this,
                "DISTRIBUTED Error on acquiring uncommitted record %s from other servers. The database could be unaligned with others!",
                e, rid);
      }
    }
  }

  public Map<String, Object> distributeOperation(final byte operation, final ORecordId rid, final OAbstractDistributedTask<?> iTask) {
    final Set<String> targetNodes = cluster.getRemoteNodeIdsBut(iTask.getNodeSource());
    if (!targetNodes.isEmpty()) {
      // RESET THE SOURCE TO AVOID LOOPS
      iTask.setNodeSource(cluster.getLocalNodeId());
      iTask.setStatus(STATUS.REMOTE_EXEC);
      return cluster.sendOperation2Nodes(targetNodes, iTask);
    }
    return null;
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
    return storageName;
  }

  public static String getClusterNameByRID(final OStorage iStorage, final ORecordId iRid) {
    final OCluster cluster = iStorage.getClusterById(iRid.clusterId);
    return cluster != null ? cluster.getName() : "*";
  }

  protected OStorage openStorage(final String iName) {
    OStorage stg = Orient.instance().getStorage(iName);
    if (stg == null) {
      // NOT YET OPEN: OPEN IT NOW
      OLogManager.instance().warn(this, "DISTRIBUTED Initializing storage '%s'", iName);

      final String url = OServerMain.server().getStorageURL(iName);
      if (url == null)
        throw new IllegalArgumentException("Database '" + iName + "' is not configured on local server");
      stg = Orient.instance().loadStorage(url);
      stg.open(null, null, null);
    }
    return stg;
  }
}
