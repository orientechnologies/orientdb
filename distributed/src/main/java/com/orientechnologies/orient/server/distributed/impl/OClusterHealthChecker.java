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
package com.orientechnologies.orient.server.distributed.impl;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.ODistributedDatabase;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedResponse;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.OLoggerDistributed;
import com.orientechnologies.orient.server.distributed.impl.task.OUpdateDatabaseSequenceStatusTask;
import com.orientechnologies.orient.server.distributed.task.ODistributedOperationException;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;
import java.util.List;
import java.util.Optional;

/**
 * Timer task that checks periodically the cluster health status.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class OClusterHealthChecker implements Runnable {
  private static final OLoggerDistributed logger =
      OLoggerDistributed.logger(OClusterHealthChecker.class);
  private final ODistributedServerManager manager;
  private final long healthCheckerEveryMs;
  private long lastExecution = 0;

  public OClusterHealthChecker(
      final ODistributedServerManager manager, final long healthCheckerEveryMs) {
    this.manager = manager;
    this.healthCheckerEveryMs = healthCheckerEveryMs;
  }

  public synchronized void run() {
    logger.debug("Checking cluster health...");

    final long now = System.currentTimeMillis();

    if (now - lastExecution > (healthCheckerEveryMs / 3)) {
      // CHECK CURRENT STATUS OF DBS
      try {
        notifyDatabaseSequenceStatus();

      } catch (HazelcastInstanceNotActiveException e) {
        // IGNORE IT
      } catch (Exception t) {
        if (manager.getServerInstance().isActive())
          logger.error("Error on checking cluster health", t);
        else
          // SHUTDOWN IN PROGRESS
          logger.debug("Error on checking cluster health", t);
      } finally {
        logger.debug("Cluster health checking completed");
      }
    } else
      logger.debug(
          "Cluster health finished recently (%dms ago), skip this execution", now - lastExecution);

    lastExecution = now;
  }

  private void notifyDatabaseSequenceStatus() {
    OServer server = manager.getServerInstance();
    OrientDBDistributed context = (OrientDBDistributed) server.getDatabases();
    if (!context.getNodeState().getOps().getNetworkTopology().isSelfEnstablished())
      // ONLY ONLINE NODE CAN TRY TO RECOVER FOR SINGLE DB STATUS
      return;
    var dbTopology = context.getNodeState().getOps().getDatabaseTopology();
    if (!server.isActive()) return;

    for (var dbId : dbTopology.getDatabases()) {
      var dbName = dbTopology.getDatabaseName(dbId);
      final ODistributedServerManager.DB_STATUS localNodeStatus = context.getDatabaseStatus(dbName);
      if (localNodeStatus != ODistributedServerManager.DB_STATUS.ONLINE)
        // ONLY NOT_AVAILABLE NODE/DB CAN BE RECOVERED
        continue;

      final List<String> servers = context.getOnlineNodesNotLocal(dbName);

      if (servers.isEmpty()) continue;

      try {
        ODistributedDatabase sharedDb = manager.getDatabase(dbName);
        if (sharedDb != null) {
          Optional<OTransactionSequenceStatus> status =
              context
                  .getSharedDatabasecontext(dbName)
                  .map((x) -> x.getTransactionSequence().currentStatus());
          if (status.isPresent()) {
            ORemoteTask task = new OUpdateDatabaseSequenceStatusTask(dbName, status.get());

            final ODistributedResponse response = manager.sendRequest(dbName, servers, task);
          }
        }
      } catch (ODistributedException e) {
        // NO SERVER RESPONDED, THE SERVER COULD BE ISOLATED: SET ALL THE SERVER AS OFFLINE
        logger.debugNode(
            manager.getLocalNodeName(), "Error on sending request for cluster health check", e);
      } catch (ODistributedOperationException e) {
        // NO SERVER RESPONDED, THE SERVER COULD BE ISOLATED: SET ALL THE SERVER AS OFFLINE
        logger.debugNode(
            manager.getLocalNodeName(), "Error on sending request for cluster health check", e);
      }
    }
  }
}
